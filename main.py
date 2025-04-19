from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pipeline_functions import ingest_hdfs, bronze_creation, silver_creation, gold_creation
# from pipeline_functions_jetstream import ingest_hdfs, bronze_creation, silver_creation, gold_creation
import time
import os
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from py4j.protocol import Py4JJavaError


# client = InsecureClient("http://localhost:9870/")
client = InsecureClient("http://node-master:9870/")
local_dir = os.path.expanduser("~/JetStream2_Benchmarking/data")
                               
spark = SparkSession.builder \
    .appName("Data Pipeline") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200")\
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
def clean_hive_tables():
    for layer in ["bronze_layer", "silver_layer", "gold_layer"]:
        spark.sql(f"USE {layer}")
        tables = spark.sql("SHOW TABLES").collect()
        for row in tables:
            table = row['tableName']
            print(f"Dropping {layer}.{table}")
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            
clean_hive_tables()
#Removes old tables before ingesting new bronze tables (for testing purposes)
spark.sql("USE DATABASE bronze_layer")
spark.sql("SHOW TABLES").show()
#spark.sql("DROP TABLE recommissioned_batteries_bz")
#spark.sql("DROP TABLE regular_alt_batteries_bz")
#spark.sql("DROP TABLE second_life_batteries_bz")
#spark.sql("SHOW TABLES").show()
#spark.sql("SELECT count(*) FROM recommissioned_batteries_bz").show()
#spark.sql("SELECT * FROM recommissioned_batteries_bz limit 10").show()


if __name__ == "__main__":
    result = []
    for run in range(30):
        try:
            total_start_time = time.time()
            t0, cpu0, mem0, read0, write0, throughput0 = ingest_hdfs(client, local_dir)
            t1, cpu1, mem1, read1, write1, throughput1 = bronze_creation(local_dir, spark)
            t2, cpu2, mem2, read2, write2, throughput2 = silver_creation(spark)
            t3, cpu3, mem3, read3, write3, throughput3 = gold_creation(spark)
            total_end_time = time.time()

            total_time = round(total_end_time - total_start_time, 2)
            result.append([
                run+1, total_time,
                t0, cpu0, mem0, read0, write0, throughput0,
                t1, cpu1, mem1, read1, write1, throughput1,
                t2, cpu2, mem2, read2, write2, throughput2,
                t3, cpu3, mem3, read3, write3, throughput3
            ])



            print(f"Run {run+1} finished in {np.round(total_end_time - total_start_time,2)} seconds")
        except Py4JJavaError as e:
            print(f"Run {run+1} failed with error:")
            break
    cols = [
        "run", "total_time",
        "t_ingest", "cpu_ingest", "mem_ingest", "read_ingest", "write_ingest", "throughput_ingest",
        "t_bronze", "cpu_bronze", "mem_bronze", "read_bronze", "write_bronze", "throughput_bronze",
        "t_silver", "cpu_silver", "mem_silver", "read_silver", "write_silver", "throughput_silver",
        "t_gold", "cpu_gold", "mem_gold", "read_gold", "write_gold", "throughput_gold"
    ]
    df = pd.DataFrame(result, columns=cols)
    output_path = "pipeline_results.csv"
    df.to_csv(output_path, index=False)