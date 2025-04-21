# JetStream2_Benchmarking

## Overview

This project benchmarks a Spark-Hive data pipeline deployed across a distributed Hadoop cluster. The pipeline consists of three layers: Bronze (raw ingestion), Silver (cleaning and transformation), and Gold (final aggregation). System metrics such as CPU usage, memory usage, disk I/O, and throughput are logged for performance evaluation.

## Technologies Used

- **Apache Spark**: 3.4.4
- **Apache Hive**: 4.0.1
- **Apache Hadoop**: 3.3.0
- **Python**: 3.10+
- **psutil**: for system monitoring

---

## Setup

1. Clone the repository:

```bash
git clone <repo_url>
cd JetStream2_Benchmarking
```

2. Set up virtual environment:

```bash
python3 -m venv venv
# Modify venv/bin/activate with appropriate paths:
export HADOOP_HOME=~/hadoop/hadoop-3.3.0
export HIVE_HOME=~/hive
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
source venv/bin/activate
pip install hdfs numpy pyspark
```

3. [Download the dataset](https://drive.google.com/drive/folders/1ZgM2wCeNH2hSc5HEfNSniGQDmmXkhsZi?usp=drive_link) and place it inside the project directory under `data/`.

4. Initialize Hadoop and Spark:

```bash
start-all.sh
$SPARK_HOME/sbin/start-all.sh
```

---

## Running the Pipeline

Run `main.py` to execute the full pipeline:

```bash
python main.py
```

---

## Pipeline Breakdown

### Ingest to HDFS

- Clears `/landing` on HDFS.
- Uploads all files from `data/` to `hdfs:///landing/`.

### Bronze Layer

- Reads raw CSVs from `/landing/`.
- Adds `battery_number` and `created_at` columns.
- Writes to Hive tables in `bronze_layer`.

### Silver Layer

- Cleans data (removes nulls, trims strings, casts types).
- Removes duplicates across key columns.
- Writes cleaned data to Hive tables in `silver_layer`.

### Gold Layer

- Merges all Silver tables.
- Adds a `battery_type` label.
- Writes final result to `gold_layer.all_batteries_gold`.

---

## Benchmarking Setup

Experiments were conducted with the following cluster configurations:

| Configuration           | Description                  |
|-------------------------|------------------------------|
| 1 node, 8 CPU cores     | Single-node baseline         |
| 1 node, 16 CPU cores    | Increased parallelism        |
| 2 nodes, 8+8 CPU cores  | Horizontal scalability test  |
| 3 nodes, 8+8+8 CPU cores| Distributed load balancing   |

Each configuration was tested over 30 pipeline runs. Performance metrics were recorded in `pipeline_results.csv`, including:

- Total runtime per stage
- Average CPU and memory usage
- Disk read/write in MB
- Throughput in MB/s

Use the CSV to analyze scalability trends and identify system bottlenecks.

---

## Output

Final Hive table: `gold_layer.all_batteries_gold`

Intermediate outputs and metrics are saved to:
- `pipeline_results.csv`
- Hive tables in `bronze_layer`, `silver_layer`, and `gold_layer`

---

## Notes

- Ensure all Hadoop and Spark daemons are running.
- If any run fails, partial tables should be manually dropped before re-running.
