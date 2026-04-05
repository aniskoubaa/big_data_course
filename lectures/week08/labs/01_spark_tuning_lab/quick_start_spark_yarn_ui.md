# Quick Start: Spark & YARN Web UIs + Running Your First Job

## 1. Accessing the Cluster

SSH into the master node using your SSH key (password login is disabled):

```bash
ssh -i <your-ssh-key> <your-username>@<master-node>
```

> Your instructor will provide the SSH key and cluster address. All commands below assume you are on the **master node**.

## 2. Web Interfaces

All UIs are accessible via HTTPS (credentials provided by your instructor):

| Interface | URL | What It Shows |
|-----------|-----|---------------|
| **HDFS NameNode** | https://hdfs.aniskoubaa.org/ | File system browser, node health, storage usage |
| **YARN ResourceManager** | https://hdfs.aniskoubaa.org/yarn/ | Running/completed applications, cluster resources, node status |
| **Spark History Server** | https://hdfs.aniskoubaa.org/spark-history/ | Completed Spark job details: stages, tasks, DAG, executor metrics |
| **Spark Web UI (live)** | https://hdfs.aniskoubaa.org/spark-ui/ | **Live** job info (only available while a Spark job is running) |

### What to Look For

- **YARN UI** (`8088`): Click **Applications** to see submitted jobs, their status, memory/CPU usage, and logs.
- **Spark History UI** (`18080`): Click a completed application to see the DAG, stages, shuffle read/write, and task distribution.
- **Spark Application UI** (`4040`): Only active during a running job. Shows live stages, storage (cached data), and the SQL execution plan.

## 3. Available Sample Data on HDFS

```bash
hdfs dfs -ls /data/
```

| File | Size | Description |
|------|------|-------------|
| `chicago_crimes.csv` | ~174 MB | Full Chicago crimes dataset |
| `chicago_crimes_sample.csv` | ~2.3 MB | Small sample (good for quick tests) |

Columns: `ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrest, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated On, Latitude, Longitude, Location`

## 4. Running a Simple Spark Job (Client Mode)

### Option A: Interactive PySpark Shell

```bash
pyspark --master yarn --num-executors 2 --executor-memory 768m
```

Then in the shell:

```python
df = spark.read.csv("/data/chicago_crimes_sample.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

# Count crimes by type
df.groupBy("Primary Type").count().orderBy("count", ascending=False).show(10)
```

Type `exit()` to quit.

### Option B: Submit a Script

Create a file `test_job.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SE446 Quick Test") \
    .getOrCreate()

df = spark.read.csv("/data/chicago_crimes_sample.csv", header=True, inferSchema=True)

print(f"Total records: {df.count()}")
print(f"Columns: {df.columns}")

# Top 10 crime types
df.groupBy("Primary Type").count() \
    .orderBy("count", ascending=False) \
    .show(10)

spark.stop()
```

Submit it:

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 768m \
    --executor-cores 1 \
    --driver-memory 512m \
    test_job.py
```

> **Why client mode?** The Driver runs on your machine so you see `print()` output directly in the terminal. Use `--deploy-mode cluster` for long unattended jobs (output goes to YARN logs instead).

## 5. Checking Results in the Web UIs

After submitting the job:

1. **While running** — open https://hdfs.aniskoubaa.org/yarn/, click your application under **Running**. Also open https://hdfs.aniskoubaa.org/spark-ui/ to see the live Spark UI.
2. **After completion** — open https://hdfs.aniskoubaa.org/spark-history/ and click the application to inspect stages, tasks, and performance.
3. **View YARN logs** (if using cluster mode):
   ```bash
   yarn logs -applicationId <app-id>
   ```

## 6. Cluster Specs (for Resource Planning)

| Component | Value |
|-----------|-------|
| Spark version | 3.5.4 |
| Worker nodes | 2 |
| Memory per worker (YARN) | 1536 MB |
| Default executor memory | 768 MB |
| Default executor instances | 2 |
| Default executor cores | 1 |
| Serializer | Kryo |

> Do not request more than **768m per executor** or **1 core per executor** — the cluster is small. Requesting too much will cause YARN to queue your job indefinitely.

## 7. Cluster Configuration Reference

### HDFS Health (as of 2026-04-01)

| Metric | Value |
|--------|-------|
| Configured Capacity | 94.78 GB |
| DFS Used | 1.18 GB (1.4%) |
| DFS Remaining | 82.50 GB |
| Under-replicated blocks | 0 |
| Corrupt blocks | 0 |
| Missing blocks | 0 |

### YARN Cluster

| Metric | Value |
|--------|-------|
| Total Nodes | 2 (both RUNNING) |
| Memory per NodeManager | 1536 MB |
| Min container allocation | 256 MB |
| Max container allocation | 1536 MB |

### Spark Configuration (`spark-defaults.conf`)

```properties
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              512m
spark.executor.memory            768m
spark.executor.instances         2
spark.executor.cores             1
spark.yarn.am.memory             256m
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs
spark.history.fs.logDirectory    hdfs:///spark-logs
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.warehouse.dir          hdfs:///user/hive/warehouse
spark.yarn.jars                  local:/opt/spark/jars/*
```
