from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pipeline_functions import ingest_hdfs, bronze_creation, silver_creation, gold_creation
# from pipeline_functions_jetstream import ingest_hdfs, bronze_creation, silver_creation, gold_creation
import time
import os
import numpy as np
from pyspark.sql import functions as F


client = InsecureClient("http://localhost:9870/")
# client = InsecureClient("http://node-master:9870/")
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
#Removes old tables before ingesting new bronze tables (for testing purposes)
spark.sql("USE DATABASE bronze_layer")
spark.sql("SHOW TABLES").show()
# spark.sql("DROP TABLE recommissioned_batteries_bz")
# spark.sql("DROP TABLE regular_alt_batteries_bz")
# spark.sql("DROP TABLE second_life_batteries_bz")
# spark.sql("SHOW TABLES").show()
#spark.sql("SELECT count(*) FROM recommissioned_batteries_bz").show()
#spark.sql("SELECT * FROM recommissioned_batteries_bz limit 10").show()


if __name__ == "__main__":
    try:
       total_start_time = time.time()
       ingest_hdfs(client, local_dir)
       bronze_creation(local_dir, spark)
       silver_creation(spark)
       gold_creation(spark)
       total_end_time = time.time()

       print(f"The pipeline finished in {np.round(total_end_time - total_start_time,2)} seconds")
    except Exception as e:
       print(e)
    