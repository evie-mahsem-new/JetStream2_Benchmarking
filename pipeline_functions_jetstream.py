import os
import time
import numpy as np
import psutil  # Import psutil for system resource monitoring
from pyspark.sql import functions as F

# This function now only appends the usage data without printing
def log_system_usage(cpu_list, memory_list, disk_read_list, disk_write_list):
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage
    memory = psutil.virtual_memory()  # Memory usage
    memory_usage = memory.percent  # Percentage of used memory
    disk_io = psutil.disk_io_counters()  # Disk I/O
    disk_read = disk_io.read_bytes  # Disk read in bytes
    disk_write = disk_io.write_bytes  # Disk write in bytes
    
    # Append the usage data to the respective lists
    cpu_list.append(cpu_usage)
    memory_list.append(memory_usage)
    disk_read_list.append(disk_read)
    disk_write_list.append(disk_write)

def ingest_hdfs(client, local_dir):
    ingest_start = time.time()

    # Accumulators for CPU, memory, and disk I/O
    cpu_usage_list = []
    memory_usage_list = []
    disk_read_list = []
    disk_write_list = []

    try:
        client.list('/')
        print("Successfully connected to HDFS.")
        if not client.status('/landing/', strict=False):
            for root, _, files in os.walk(local_dir):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    
                    relative_path = os.path.relpath(local_file_path, local_dir)
                    hdfs_file_path = os.path.join("/landing/", relative_path)

                    hdfs_dir = os.path.dirname(hdfs_file_path)
                    client.makedirs(hdfs_dir)

                    try:
                        with open(local_file_path, "rb") as local_file:
                            with client.write(hdfs_file_path, overwrite=True) as writer:
                                writer.write(local_file.read())
                        print(f"Uploaded: {local_file_path} -> {hdfs_file_path}")
                    except Exception as e:
                        print(f"Error uploading {file}: {e}")
                    
                    # Log system usage after processing each file (no print, just collect data)
                    log_system_usage(cpu_usage_list, memory_usage_list, disk_read_list, disk_write_list)
                              
        else:
            print("Data is already in Landing")
    except Exception as e:
        print(f"Error connecting to HDFS: {e}")
        return
    ingest_end = time.time()

    # Calculate average CPU and memory usage, and total disk read/write
    avg_cpu_usage = np.mean(cpu_usage_list)
    avg_memory_usage = np.mean(memory_usage_list)
    total_disk_read = np.sum(disk_read_list)
    total_disk_write = np.sum(disk_write_list)

    print(f"All files uploaded successfully to the HDFS landing directory with subdirectories in {np.round(ingest_end-ingest_start, 2)} seconds!")

    # Log summary of system usage
    print(f"\nAverage CPU Usage: {avg_cpu_usage:.2f}%")
    print(f"Average Memory Usage: {avg_memory_usage:.2f}%")
    print(f"Total Disk Read: {total_disk_read / (1024 ** 2):.2f} MB")
    print(f"Total Disk Write: {total_disk_write / (1024 ** 2):.2f} MB")

def bronze_creation(local_dir, spark_context):
    bronze_start = time.time()

    # Accumulators for CPU, memory, and disk I/O
    cpu_usage_list = []
    memory_usage_list = []
    disk_read_list = []
    disk_write_list = []
    
    # Ensure the database exists
    spark_context.sql("CREATE DATABASE IF NOT EXISTS bronze_layer")
    spark_context.catalog.setCurrentDatabase("bronze_layer")
    
    for root, _, files in os.walk(local_dir):
        folder = root.split("/")[-1]
        table_name = f"{folder}_bz"
        if folder == 'landing' or folder == 'data':
            continue       
        # Ensure the root path is correct
        root_path = f"hdfs://node-master:9000/landing/{folder}"  # Update this to match your HDFS configuration
        
        print(f"Reading data from: {root_path}")
        
        for file in files:
            write_start = time.time()
            
            # Check if the file exists in HDFS
            file_path = f"{root_path}/{file}"
            try:
                raw_df = spark_context.read.option("header", "true").csv(file_path)
                raw_df.cache()
                raw_df = raw_df.withColumn("battery_number", F.lit(file.replace(".csv", "")))\
                               .withColumn("created_at", F.current_timestamp())
                
                # Write to Hive table (creating table if not exists)
                raw_df.write.mode("append").saveAsTable(f"bronze_layer.{table_name}")
                
                write_end = time.time()
                print(f"The file {file} was written into the table {table_name} in {np.round(write_end - write_start, 2)} seconds.")
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
            
            # Log system usage after processing each file (no print, just collect data)
            log_system_usage(cpu_usage_list, memory_usage_list, disk_read_list, disk_write_list)
    
    bronze_end = time.time()

    # Calculate average CPU and memory usage, and total disk read/write
    avg_cpu_usage = np.mean(cpu_usage_list)
    avg_memory_usage = np.mean(memory_usage_list)
    total_disk_read = np.sum(disk_read_list)
    total_disk_write = np.sum(disk_write_list)

    print(f"Bronze tables creation was finished in {np.round(bronze_end - bronze_start, 2)} seconds.")

    # Log summary of system usage
    print(f"\nAverage CPU Usage: {avg_cpu_usage:.2f}%")
    print(f"Average Memory Usage: {avg_memory_usage:.2f}%")
    print(f"Total Disk Read: {total_disk_read / (1024 ** 2):.2f} MB")
    print(f"Total Disk Write: {total_disk_write / (1024 ** 2):.2f} MB")

def silver_creation(spark_context):
    pass

def gold_creation(spark_context):
    pass
