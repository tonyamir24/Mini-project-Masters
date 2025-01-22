import pandas as pd
import shutil

# Define the time range for filtering
START_TIME = "2017-10-07 00:00:00"
END_TIME = "2017-10-08 00:00:00"

# Function to filter test-format files based on the time range
def filter_test_file(input_file, output_file, time_column, delimiter=" "):
    try:
        # Read the test-format file into a DataFrame
        df = pd.read_csv(input_file, delimiter=delimiter, on_bad_lines='skip')
        
        # Convert the time column to datetime
        df[time_column] = pd.to_datetime(df[time_column])
        
        # Filter the DataFrame based on the time range
        filtered_df = df[(df[time_column] >= START_TIME) & (df[time_column] <= END_TIME)]
        
        # Write the filtered DataFrame to a new file
        filtered_df.to_csv(output_file, index=False, sep=delimiter)
        print(f"Filtered data saved to {output_file}")
    except Exception as e:
        print(f"Error processing {input_file}: {e}")

# Filter each test-format file
# Adjust the delimiter if it's not a space (e.g., "\t" for tabs, "|" for pipes, etc.)
filter_test_file("./trace-data/cluster_gpu_util", "./trace-data/filtered_cluster_gpu_util", "time", delimiter=" ")
filter_test_file("./trace-data/cluster_cpu_util", "./trace-data/filtered_cluster_cpu_util", "time", delimiter=" ")
filter_test_file("./trace-data/cluster_mem_util", "./trace-data/filtered_cluster_mem_util", "time", delimiter=" ")

# The cluster_machine_list does not have a time column, so it doesn't need filtering
# If you want to copy it as is, you can simply copy the file
try:
    shutil.copy2("./trace-data/cluster_machine_list", "./trace-data/filtered_cluster_machine_list")
    print("Copied cluster_machine_list to filtered_cluster_machine_list")
except Exception as e:
    print(f"Error copying cluster_machine_list: {e}")