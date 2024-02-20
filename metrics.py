#!/usr/bin/python3
import csv
import subprocess
import os

def get_cgroup_pid_to_job():
    """Returns a mapping of pid to job id from cgroups"""
    pid_to_job = {}

    for subdir, _, files in os.walk("/sys/fs/cgroup"):
        for file in files:
            if file == "cgroup.procs":
                with open(os.path.join(subdir, file), 'r') as f:
                    for pid in f:
                        pid = pid.strip()
                        job_id = subdir.split('/')[-1]
                        pid_to_job[pid] = job_id

    return pid_to_job

def get_nvidia_metrics():
    """Returns gpu metrics as a dictionary from nvidia-smi commands"""
    try:
        # Run nvidia-smi commands and save output to temp files
        subprocess.run('nvidia-smi --query-compute-apps=pid,used_gpu_memory,gpu_name,gpu_uuid --format=csv > compute_apps_usage.csv', shell=True, check=True)
        subprocess.run('nvidia-smi --query-gpu=gpu_uuid,name,utilization.gpu --format=csv > gpu_usage.csv', shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running nvidia-smi commands: {e}")
        return {}

    metrics = {}
    try:
        # Read compute_apps_usage.csv
        with open('compute_apps_usage.csv', 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                pid = row[0]
                gpu_memory_usage = row[1]
                gpu_name = row[2]
                gpu_uuid = row[3]

                metrics[pid] = {
                    'gpu_memory_usage': gpu_memory_usage,
                    'gpu_name': gpu_name,
                    'gpu_uuid': gpu_uuid,
                    'gpu_utilization': 'N/A',  # Initialize with 'N/A'
                }

        # Read gpu_usage.csv and match UUIDs
        with open('gpu_usage.csv', 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                gpu_uuid = row[0]
                gpu_utilization = row[2]

                for pid, metric in metrics.items():
                    if metric['gpu_uuid'] == gpu_uuid:
                        metric['gpu_utilization'] = gpu_utilization

    except IOError as e:
        print(f"Error reading CSV files: {e}")

    return metrics

def get_io_metrics(pid_to_job):
    """Returns io metrics as a dictionary for each pid"""
    io_metrics = {}
    for pid in pid_to_job.keys():
        try:
            with open(f'/proc/{pid}/io', 'r') as f:
                io_data = f.read()
            io_metrics[pid] = parse_io_data(io_data)
        except IOError:
            io_metrics[pid] = {
                'read_bytes': 'N/A',
                'write_bytes': 'N/A'
            }
    return io_metrics

def parse_io_data(io_data):
    """Parse the io data from /proc/[pid]/io"""
    lines = io_data.split('\n')
    io_info = {}
    for line in lines:
        if 'read_bytes' in line or 'write_bytes' in line:
            key, value = line.split(':')
            io_info[key.strip()] = value.strip()
    return io_info
    
def write_to_textfile_collector(pid_to_job, metrics):
    """Writes gpu metrics to the textfile collector format"""
    job_gpu_utilization = {}  # Dictionary to store job-wise GPU utilization
    job_gpu_memory_usage = {}  # Dictionary to store job-wise GPU memory usage
    
    # Collect GPU utilization and memory usage per job_id
    for pid, job in pid_to_job.items():
        metric = metrics.get(pid, {})
        gpu_uuid = metric.get('gpu_uuid', '')
        gpu_utilization = metric.get('gpu_utilization', 'N/A')
        gpu_memory_usage = metric.get('gpu_memory_usage', 'N/A')
        
        if gpu_utilization != 'N/A':
            job_gpu_utilization.setdefault(job, []).append((gpu_uuid, gpu_utilization))
        if gpu_memory_usage != 'N/A':
            job_gpu_memory_usage.setdefault(job, []).append((gpu_uuid, gpu_memory_usage))
    
    # Write the GPU utilization and memory usage to the textfile collector
    with open('/var/lib/node_exporter/textfile_collector/gpu_metrics.prom', 'w') as f:
        for job, gpu_utilizations in job_gpu_utilization.items():
            for gpu_uuid, gpu_utilization in gpu_utilizations:
                f.write(f'cgroups_nvidia_gpu_utilization{{gpu_id="{gpu_uuid}", job_id="{job}"}} {gpu_utilization}\n')
        
        for job, gpu_memory_usages in job_gpu_memory_usage.items():
            for gpu_uuid, gpu_memory_usage in gpu_memory_usages:
                f.write(f'cgroups_nvidia_gpu_memory_usage_in_bytes{{gpu_id="{gpu_uuid}", job_id="{job}"}} {gpu_memory_usage}\n')
              
    # Write the IO metrics to the textfile collector
    with open('/var/lib/node_exporter/textfile_collector/io_metrics.prom', 'a') as f:
        for pid, job in pid_to_job.items():
            io_metric = io_metric.get(pid, {})
            read_bytes = io_metric.get('read_bytes', 'N/A')
            write_bytes = io_metric.get('write_bytes', 'N/A')

            f.write(f'cgroups_io_read_bytes{{pid="{pid}", job_id="{job}"}} {read_bytes}\n')
            f.write(f'cgroups_io_write_bytes{{pid="{pid}", job_id="{job}"}} {write_bytes}\n')


def main():
    pid_to_job = get_cgroup_pid_to_job()
    metrics = get_nvidia_metrics()
    io_metrics = get_io_metrics(pid_to_job)
    write_to_textfile_collector(pid_to_job, metrics, io_metrics)

if __name__ == "__main__":
    main()
