import json
import pandas as pd
import time
from kubernetes import client, config

# Load Kubernetes config
config.load_kube_config()

# Create API instances
batch_v1 = client.BatchV1Api()

# Load trace files
with open("filtered_cluster_job_log.json", "r") as f:
    trace = json.load(f)

gpu_util = pd.read_csv("cluster_gpu_util.csv")
cpu_util = pd.read_csv("cluster_cpu_util.csv")
mem_util = pd.read_csv("cluster_mem_util.csv")
machine_list = pd.read_csv("cluster_machine_list.csv")

# Helper functions
def estimate_cpu(machine_id, start_time, end_time):
    """Estimate CPU usage based on historical utilization."""
    cpu_data = cpu_util[(cpu_util['machine_id'] == machine_id)]
    usage = cpu_data[(cpu_data['time'] >= start_time) & (cpu_data['time'] <= end_time)]['cpu_util']
    return f"{usage.mean():.2f}m" if not usage.empty else "500m"

def estimate_memory(machine_id, start_time, end_time):
    """Estimate memory usage based on historical utilization."""
    mem_data = mem_util[(mem_util['machine_id'] == machine_id)]
    mem_usage = mem_data[(mem_data['time'] >= start_time) & (mem_data['time'] <= end_time)]
    if not mem_usage.empty:
        avg_mem_used = mem_usage['mem_total'].mean() - mem_usage['mem_free'].mean()
        return f"{avg_mem_used / 1024 / 1024:.2f}Mi"  # Convert to Mi
    return "512Mi"

def count_gpus(machine_id):
    """Get the number of GPUs on a machine."""
    gpu_row = machine_list[machine_list['machineId'] == machine_id]
    return int(gpu_row['number of GPUs'].iloc[0]) if not gpu_row.empty else 0

# Process and deploy jobs
for job in trace:
    if job["status"] != "Pass":
        continue

    job_name = job["jobid"].replace("application_", "").replace("_", "-")
    attempts = job.get("attempts", [])
    if not attempts:
        print(f"Skipping job {job_name}: No scheduling attempts.")
        continue

    first_attempt = attempts[0]
    start_time = first_attempt.get("start_time")
    end_time = first_attempt.get("end_time")
    machine_id = first_attempt["detail"][0]["ip"] if first_attempt["detail"] else None

    if not start_time or not end_time or not machine_id:
        print(f"Skipping job {job_name}: Incomplete scheduling information.")
        continue

    # Estimate resources
    cpu_request = estimate_cpu(machine_id, start_time, end_time)
    memory_request = estimate_memory(machine_id, start_time, end_time)
    gpus = count_gpus(machine_id)

    command = f"echo 'Job {job_name} running on {gpus} GPUs with {cpu_request} CPU and {memory_request} memory.'"

    # Create and deploy job
    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name},
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": job_name,
                            "image": "busybox",
                            "command": ["sh", "-c", command],
                            "resources": {
                                "requests": {"cpu": cpu_request, "memory": memory_request}
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                }
            }
        },
    }

    print(f"Deploying job {job_name}...")
    batch_v1.create_namespaced_job(namespace="default", body=job_manifest)
    time.sleep(5)  # Simulate delay between deployments
