# GPU and I/O Metrics Collection

## Metrics.py
### Overview
This script collects GPU and I/O metrics on a system using NVIDIA-SMI commands and outputs the metrics in a format that is compatible with Prometheus.

### Prerequisites 
- Python3
- Access to the `nvidia-smi` tool

## Metrics.go
### Overview
This is the Golang script that collects GPU and I/O metrics on a system using NVIDIA-SMI commands and connects thought prometheus through port 

### Setup 
To run the application: 

    ```
    go run metrics.go
    ```
    
To access the metrics:

    ```
    http://localhost:9060/mertrics 
    ```
    

Configure the prometheus instance to scrape metrics from golang application:

    ```
        scrape_configs:
        - job_name: job_metrics
          scrape_interval: 10s
          static_configs:
          - targets:
            - localhost:9060
    ```