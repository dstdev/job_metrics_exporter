package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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

	//Initalize metrics to zero
	gpuMemoryUsageMetric.WithLabelValues("none", "none").Set(0)
	gpuUtilizationMetric.WithLabelValues("none", "none").Set(0)
	ioReadBytesMetric.WithLabelValues("none", "none").Set(0)
	ioWriteBytesMetric.WithLabelValues("none", "none").Set(0)
}

func getJobIDFromPID(pid string) (string, error) {
	path := fmt.Sprintf("/proc/%s/cgroup", pid)
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "slurm") && strings.Contains(line, "job_") {
			// Extract job ID from the line
			parts := strings.Split(line, "job_")
			if len(parts) > 1 {
				jobID := strings.Split(parts[1], "/")[0]
				return jobID, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
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
				// Note: You should update gpuUtilizationMetric similarly if you have that data available.
			}
		}
	}
}

func collectIOMetrics() {
	// Iterate over PIDs and collect IO metrics
	procDir, err := os.Open("/proc")
	if err != nil {
		fmt.Printf("Failed to open /proc: %s\n", err)
		return
	}
	defer procDir.Close()

	pids, err := procDir.Readdirnames(-1)
	if err != nil {
		fmt.Printf("Failed to read /proc: %s\n", err)
		return
	}

	for _, pid := range pids {
		if _, err := strconv.Atoi(pid); err == nil {
			jobID, err := getJobIDFromPID(pid)
			if err != nil {
				fmt.Printf("Error fetching job ID for PID %s: %v\n", pid, err)
				continue
			}

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
						ioReadBytesMetric.With(prometheus.Labels{"pid": pid, "job_id": jobID}).Set(value)
					} else if key == "write_bytes" {
						ioWriteBytesMetric.With(prometheus.Labels{"pid": pid, "job_id": jobID}).Set(value)
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
