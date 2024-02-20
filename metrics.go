package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func getCgroupPidToJob() (map[string]string, error) {
	pidToJob := make(map[string]string)

	err := filepath.Walk("/sys/fs/cgroup", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || info.Name() != "cgroup.procs" {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			pid := scanner.Text()
			jobID := filepath.Base(filepath.Dir(path))
			pidToJob[pid] = jobID
		}

		return scanner.Err()
	})

	return pidToJob, err
}

func getNvidiaMetrics() (map[string]map[string]string, error) {
	metrics := make(map[string]map[string]string)

	computeAppsCmd := "nvidia-smi --query-compute-apps=pid,used_gpu_memory,gpu_name,gpu_uuid --format=csv"
	gpuUsageCmd := "nvidia-smi --query-gpu=gpu_uuid,name,utilization.gpu --format=csv"

	// Function to run a command and return its output
	runCmd := func(cmd string) ([]byte, error) {
		return exec.Command("bash", "-c", cmd).Output()
	}

	// Run the computeAppsCmd and parse the output
	computeAppsOutput, err := runCmd(computeAppsCmd)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(computeAppsOutput)))
	computeAppsRecords, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Skip header and iterate over records
	for _, record := range computeAppsRecords[1:] {
		pid := record[0]
		metrics[pid] = map[string]string{
			"gpu_memory_usage": record[1],
			"gpu_name":         record[2],
			"gpu_uuid":         record[3],
			"gpu_utilization":  "N/A", // Default value, to be updated later
		}
	}

	// Run the gpuUsageCmd and parse the output
	gpuUsageOutput, err := runCmd(gpuUsageCmd)
	if err != nil {
		return nil, err
	}

	reader = csv.NewReader(strings.NewReader(string(gpuUsageOutput)))
	gpuUsageRecords, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Skip header and iterate over records to match GPU UUIDs
	for _, record := range gpuUsageRecords[1:] {
		gpuUuid := record[0]
		gpuUtilization := record[2]

		for pid, metric := range metrics {
			if metric["gpu_uuid"] == gpuUuid {
				metrics[pid]["gpu_utilization"] = gpuUtilization
			}
		}
	}

	return metrics, nil
}

func getIOMetrics(pidToJob map[string]string) (map[string]map[string]string, error) {
	ioMetrics := make(map[string]map[string]string)

	for pid := range pidToJob {
		ioData, err := readProcIO(pid)
		if err != nil {
			// If there's an error reading a particular PID, you might choose to log it and continue
			// fmt.Printf("Error reading IO for PID %s: %v\n", pid, err)
			continue
		}
		ioMetrics[pid] = ioData
	}

	return ioMetrics, nil
}

// readProcIO reads and parses the IO data from /proc/[pid]/io
func readProcIO(pid string) (map[string]string, error) {
	path := fmt.Sprintf("/proc/%s/io", pid)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ioData := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		ioData[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ioData, nil
}

func writeMetricsToFile(metrics map[string]map[string]string) error {
	// Open a file and write metrics
	// ...
	return nil
}

func main() {
	pidToJob, err := getCgroupPidToJob()
	if err != nil {
		fmt.Println("Error getting cgroup PID to Job mapping:", err)
		return
	}

	nvidiaMetrics, err := getNvidiaMetrics()
	if err != nil {
		fmt.Println("Error getting NVIDIA metrics:", err)
		return
	}

	ioMetrics, err := getIOMetrics(pidToJob)
	if err != nil {
		fmt.Println("Error getting IO metrics:", err)
		return
	}

	if err := writeMetricsToFile(nvidiaMetrics); err != nil {
		fmt.Println("Error writing metrics to file:", err)
	}
}
