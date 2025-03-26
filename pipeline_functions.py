import os
import time
import numpy as np
from pyspark.sql import functions as F

def ingest_hdfs(client, local_dir):
    ingest_start = time.time()    
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
                              
        else:
            print("Data is already in Landing")
    except Exception as e:
        print(f"Error connecting to HDFS: {e}")
        return
    ingest_end = time.time()
    print(f"All files uploaded successfully to the HDFS landing directory with subdirectories in {np.round(ingest_end-ingest_start, 2)} seconds!")


def bronze_creation(local_dir, spark_context):
    bronze_start = time.time()
    spark_context.sql(f"CREATE DATABASE IF NOT EXISTS bronze_layer")
    spark_context.catalog.setCurrentDatabase("bronze_layer")

    for root, _, files in os.walk(local_dir):
        folder = root.split("/")[-1]
        table_name = f"{folder}_bz"
        root_path = f"hdfs://localhost:9000/landing/{folder}"

        for file in files:
            write_start = time.time()
            raw_df = spark_context.read.option("header", "true").csv(f"{root_path}/{file}")
            raw_df.cache()
            raw_df = raw_df.withColumn("battery_number", F.lit(file.replace(".csv", ""))).withColumn("created_at", F.current_timestamp())
            raw_df.write.mode("append").saveAsTable(f"bronze_layer.{table_name}")
            write_end = time.time()

            print(f"The file {file} was written into the table {table_name} in {np.round(write_end-write_start,2)} seconds.")

    bronze_end = time.time()
    print(f"Bronze tables creation was finished in {np.round(bronze_end-bronze_start, 2)} seconds.")


def silver_creation(spark_context):
    pass

def gold_creation(spark_context):
    pass

