import json

# Define a small time range for filtering jobs
START_TIME = "2017-10-07 00:00:00"
END_TIME = "2017-10-08 00:00:00"

def filter_jobs(input_file, output_file):
    with open(input_file, 'r') as f:
        data = json.load(f)

    filtered_jobs = []
    for job in data:
        if job.get("submitted_time"):
            if START_TIME <= job["submitted_time"] <= END_TIME:
                filtered_jobs.append(job)

    with open(output_file, 'w') as f:
        json.dump(filtered_jobs, f, indent=4)

# Use the function
filter_jobs("cluster_job_log", "filtered_cluster_job_log.json")
