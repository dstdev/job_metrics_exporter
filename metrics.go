package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	gpuUtilizationMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpu_utilization",
		Help: "GPU utilization percentage.",
	}, []string{"gpu_id", "job_id"})

	gpuMemoryUsageMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpu_memory_usage_bytes",
		Help: "GPU memory usage in bytes.",
	}, []string{"gpu_id", "job_id"})

	ioReadBytesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "io_read_bytes",
		Help: "IO read bytes.",
	}, []string{"pid", "job_id"})

	ioWriteBytesMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "io_write_bytes",
		Help: "IO write bytes.",
	}, []string{"pid", "job_id"})
)

func init() {
	// Register the custom metrics with Prometheus's default registry
	prometheus.MustRegister(gpuUtilizationMetric)
	prometheus.MustRegister(gpuMemoryUsageMetric)
	prometheus.MustRegister(ioReadBytesMetric)
	prometheus.MustRegister(ioWriteBytesMetric)

	//Initalize metrics to zero for trakcing 0% utilization
	gpuMemoryUsageMetric.WithLabelValues("none", "none").Set(0)
	gpuUtilizationMetric.WithLabelValues("none", "none").Set(0)
	ioReadBytesMetric.WithLabelValues("none", "none").Set(0)
	ioWriteBytesMetric.WithLabelValues("none", "none").Set(0)
}

//Added function to get current runing jobs

// getJobIDFromPID finds the job ID for a given PID from the Slurm cgroup directory
func getJobIDFromPID(pid string) (string, error) {
	// Base path to Slurm
	basePath := "/sys/fs/cgroup/cpu/slurm"

	// Open the base path directory
	baseDir, err := os.Open(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to open the base directory: %v", err)
	}
	defer baseDir.Close()

	// Read entries
	entries, err := baseDir.Readdirnames(-1)
	if err != nil {
		return "", fmt.Errorf("failed to read the entries in the directory: %v", err)
	}

	// Iterate over each entry looking for uid directories
	for _, entry := range entries {
		if strings.HasPrefix(entry, "uid_") {
			// Construct path for this uid directory
			uidPath := fmt.Sprintf("%s/%s", basePath, entry)

			// Open the uid directory
			uidDir, err := os.Open(uidPath)
			if err != nil {
				continue // If unable to open, skip to next uid directory
			}

			// Read job entries in the uid directory
			jobEntries, err := uidDir.Readdirnames(-1)
			uidDir.Close()
			if err != nil {
				continue // If unable to read, skip to next uid directory
			}

			// Iterate over job entries
			for _, jobEntry := range jobEntries {
				if strings.HasPrefix(jobEntry, "job_") {
					// Construct path for this job directory
					jobPath := fmt.Sprintf("%s/%s/cgroup.procs", uidPath, jobEntry)

					// Attempt to open the cgroup.procs file within this job directory
					file, err := os.Open(jobPath)
					if err != nil {
						continue // If unable to open, skip to next job directory
					}

					// Scan through the cgroup.procs file
					scanner := bufio.NewScanner(file)
					for scanner.Scan() {
						line := scanner.Text()
						if line == pid {
							file.Close()
							return strings.TrimPrefix(jobEntry, "job_"), nil
						}
					}
					file.Close()

					if err := scanner.Err(); err != nil {
						return "", fmt.Errorf("error scanning cgroup file for PID %s in %s: %v", pid, jobPath, err)
					}
				}
			}
		}
	}

	return "", fmt.Errorf("job ID not found for PID %s", pid)
}

func collectGPUMetrics() {
	// Run nvidia-smi to get GPU usage and application memory usage
	gpuInfoCmd := exec.Command("bash", "-c", "nvidia-smi --query-gpu=gpu_uuid,index,name,utilization.gpu --format=csv,noheader")
	gpuInfoOutput, err := gpuInfoCmd.Output()
	if err != nil {
		fmt.Printf("Failed to execute command: %s\n", err)
		return
	}

	computeAppsCmd := exec.Command("bash", "-c", "nvidia-smi --query-compute-apps=pid,used_gpu_memory,gpu_uuid --format=csv,noheader")
	computeAppsOutput, err := computeAppsCmd.Output()
	if err != nil {
		fmt.Printf("Failed to execute command: %s\n", err)
		return
	}

	// Process gpuInfoOutput to map UUID to GPU ID
	gpuInfoLines := strings.Split(strings.TrimSpace(string(gpuInfoOutput)), "\n")
	gpuUUIDToIndex := make(map[string]string)
	for _, line := range gpuInfoLines {
		parts := strings.Split(line, ", ")
		if len(parts) == 4 {
			uuid := parts[0]
			index := parts[1]
			gpuUUIDToIndex[uuid] = index
		}
	}

	// Process computeAppsOutput and update Prometheus metrics
	computeAppsLines := strings.Split(strings.TrimSpace(string(computeAppsOutput)), "\n")
	for _, line := range computeAppsLines {
		parts := strings.Split(line, ", ")
		if len(parts) == 3 {
			pid := parts[0]
			usedMemory, err := strconv.ParseFloat(strings.Trim(parts[1], " MiB"), 64)
			if err != nil {
				fmt.Printf("Error parsing used GPU memory for PID %s: %v\n", pid, err)
				continue
			}
			uuid := parts[2]

			if index, exists := gpuUUIDToIndex[uuid]; exists {
				jobID, err := getJobIDFromPID(pid)
				if err != nil {
					fmt.Printf("Error fetching job ID for PID %s: %v\n", pid, err)
					continue
				}

				gpuMemoryUsageMetric.With(prometheus.Labels{"gpu_id": index, "job_id": jobID}).Set(usedMemory * 1024 * 1024) // Convert MiB to bytes
			}
		}
	}
}

func collectIOMetrics() {
	// Base path to Slurm
	basePath := "/sys/fs/cgroup/cpu/slurm"

	// Open the base path directory
	baseDir, err := os.Open(basePath)
	if err != nil {
		fmt.Printf("Failed to open the base directory: %s\n", err)
		return
	}
	defer baseDir.Close()

	// Read entries
	entries, err := baseDir.Readdirnames(-1)
	if err != nil {
		fmt.Printf("Failed to read the entries in the directory: %s\n", err)
		return
	}

	// Iterate over each entry looking for uid directories
	for _, entry := range entries {
		if strings.HasPrefix(entry, "uid_") {
			// Construct path for this uid directory
			uidPath := fmt.Sprintf("%s/%s", basePath, entry)

			// Open the uid directory
			uidDir, err := os.Open(uidPath)
			if err != nil {
				fmt.Printf("Failed to open UID directory %s: %s\n", uidPath, err)
				continue // If unable to open, skip to next uid directory
			}

			// Read job entries in the uid directory
			jobEntries, err := uidDir.Readdirnames(-1)
			uidDir.Close()
			if err != nil {
				fmt.Printf("Failed to read job entries in UID directory %s: %s\n", uidPath, err)
				continue // If unable to read, skip to next uid directory
			}

			// Iterate over job entries
			for _, jobEntry := range jobEntries {
				if strings.HasPrefix(jobEntry, "job_") {
					// Construct path for this job directory
					jobPath := fmt.Sprintf("%s/%s", uidPath, jobEntry)
					cgroupProcsPath := filepath.Join(jobPath, "cgroup.procs")

					// Check if cgroup.procs file exists
					if _, err := os.Stat(cgroupProcsPath); os.IsNotExist(err) {
						fmt.Printf("No cgroup.procs file for job %s (UID %s), skipping\n", jobEntry, entry)
						continue
					}

					// Read the PIDs from the cgroup.procs file
					pids, err := os.ReadFile(cgroupProcsPath)
					if err != nil {
						fmt.Printf("Failed to read cgroup.procs for job %s (UID %s): %v\n", jobEntry, entry, err)
						continue
					}

					// If no PIDs, initialize I/O metrics to zero and skip this job
					if len(strings.Fields(string(pids))) == 0 {
						fmt.Printf("No PIDs found in cgroup.procs for job %s (UID %s), initializing I/O metrics to zero\n", jobEntry, entry)
						ioReadBytesMetric.With(prometheus.Labels{"pid": "none", "job_id": jobEntry}).Set(0)
						ioWriteBytesMetric.With(prometheus.Labels{"pid": "none", "job_id": jobEntry}).Set(0)
						continue
					}

					// Collect I/O metrics for each PID
					for _, pid := range strings.Fields(string(pids)) {
						ioFilePath := fmt.Sprintf("/proc/%s/io", pid)
						content, err := os.ReadFile(ioFilePath)
						if err != nil {
							fmt.Printf("Error reading IO file for PID %s: %v\n", pid, err)
							continue
						}

						for _, line := range strings.Split(string(content), "\n") {
							parts := strings.Split(line, ":")
							if len(parts) == 2 {
								key := strings.TrimSpace(parts[0])
								value, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
								if err != nil {
									fmt.Printf("Error parsing IO metric for PID %s: %v\n", pid, err)
									continue
								}

								if key == "read_bytes" {
									ioReadBytesMetric.With(prometheus.Labels{"pid": pid, "job_id": jobEntry}).Set(value)
								} else if key == "write_bytes" {
									ioWriteBytesMetric.With(prometheus.Labels{"pid": pid, "job_id": jobEntry}).Set(value)
								}
							}
						}
					}
				}
			}
		}
	}
}

func main() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				collectGPUMetrics()
				collectIOMetrics()
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	fmt.Println("Serving metrics at /metrics")
	//Exposes metrics via http://localhost:9060/metrics
	http.ListenAndServe(":9060", nil)
}
