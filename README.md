# JetStream2_Benchmarking

## Setup:

For this project, Hive 4.0.1, Spark 3.4.4, and Hadoop 3.3.0 are utilized.

Step 1: You should do a git clone of this repository. 

Step 2: You must run the following command within the repository to create your venv:
```
  2.1: python3 -m venv venv
  2.2: In the activate file within venv/bin, add the following (modify if paths are different): 
      export HADOOP_HOME=~/hadoop/hadoop-3.3.0 
      export HIVE_HOME=~/hive
      export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  2.3: source venv/bin/activate
  2.4: pip install hdfs, numpy, pyspark
```

Step 3: The data is too big to be uploaded to GitHub, so you will need to download the data from this [Google Drive](https://drive.google.com/drive/folders/1ZgM2wCeNH2hSc5HEfNSniGQDmmXkhsZi?usp=drive_link). This data is to be placed within the JetStream2_Benchmarking folder.

Step 4: Run `main.py` to run the pipeline. 

## Initiate hadoop and spark

start-all.sh

$SPARK_HOME/sbin/start-all.sh

## Ingest Data:

Make sure remove data on hdfs first

hdfs dfs -rm -r /landing

This function loops through all of the data within the data directory and uploads it into a hdfs:///landing/ directory.

## Bronze Layer:

This function loops through all data within the landing, creates a bronze layer database, loads data into a dataframe, and then writes it to the tables. 

## Silver Layer

## Gold Layer

