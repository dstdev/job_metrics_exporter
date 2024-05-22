![Build and Test](https://github.com/dstdev/job_metrics_exporter/actions/workflows/commit.yml/badge.svg)
# GPU and I/O Metrics Collection

## Metrics.go
### Overview
This is the Golang script that collects GPU and I/O metrics on a system using NVIDIA-SMI commands and connects thought prometheus through port 

### Setup Instructions
#### Prerequistes 
- A system with access to the `nvidia-smi` tool
- Allow traffic on port 9060

#### Build the application
To build the executable file for the application, run: 

```
make build
```

#### Clean UP
To clean up the build files and cache, run:

```
make clean
```
#### Running the application   
After building the application, you can start the application:
    
```
./job_metrics_exporter
```

#### Accessing Metrics
To access the metrics:

```
http://localhost:9060/metrics 
```
    
#### Configuring Prometheus
Configure the prometheus instance to scrape metrics from golang application:

```
    scrape_configs:
    - job_name: job_metrics
        scrape_interval: 60s
        static_configs:
        - targets:
        - localhost:9060
```

## Metrics.py
### Overview
This script collects GPU and I/O metrics on a system using NVIDIA-SMI commands and outputs the metrics in a format that is compatible with Prometheus.

### Prerequisites 
- Python3
- Access to the `nvidia-smi` tool
