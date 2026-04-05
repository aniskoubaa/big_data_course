# Week 8: Spark on the Cluster + Performance Tuning

## Overview

Week 8 has a single session (Wednesday). **Monday was the Midterm Exam.** **Session 8A** covers deploying Spark jobs on a YARN cluster using `spark-submit`, reading the Spark Web UI, and applying performance optimizations (caching, broadcast joins, partition tuning).

---

## Session 8A: Spark on the Cluster + Performance Tuning

### Learning Objectives
1. Submit Spark jobs to a YARN cluster using `spark-submit`
2. Read the Spark Web UI: jobs, stages, tasks, executor metrics
3. Understand partitions, parallelism, and the shuffle cost
4. Apply key optimizations: caching (`.cache()` / `.persist()`), broadcast joins, partition tuning
5. Interpret Spark execution plan (DAG) and identify bottlenecks
6. Compare job speed with and without caching on the Chicago crimes dataset

### Pre-Class Video
**"Spark Performance Tuning"** - ByteByteGo (~15 min)
🔗 https://www.youtube.com/watch?v=daXEp4HmS-E

**Alternative**: "Apache Spark Tuning & Optimization" - Simplilearn (~25 min)
🔗 https://www.youtube.com/watch?v=rNQGBkHnPUI

### Materials
- 📊 Slides: `slides/current/SE446_W08A_spark_cluster.pdf`
- 📓 Notebook: `notebooks/SE446_W08A_spark_cluster.ipynb`
- 🔬 Lab: `labs/01_spark_tuning_lab/`

---

## Key Concepts

### Spark Job Submission

```bash
# Submit a PySpark script to YARN
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 1g \
    --executor-cores 2 \
    my_script.py

# Deploy modes
#   client: Driver runs on your machine (good for interactive debugging)
#   cluster: Driver runs inside YARN (good for production / unattended jobs)
```

### Caching & Persistence

```python
# .cache() = persist in RAM only (default)
df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)
df.cache()          # marks for caching; actual caching happens on first action
df.count()          # first action → reads from HDFS, stores in RAM
df.groupBy("Primary Type").count().show()  # second action → reads from RAM (fast!)

# .persist() with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)   # spill to disk if RAM is full
df.persist(StorageLevel.DISK_ONLY)         # disk only (for very large datasets)

# Unpersist when done
df.unpersist()
```

### Storage Level Comparison

| Storage Level | RAM | Disk | Serialized? | Replicated? | Use Case |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | ✅ | ❌ | No | No | Default (.cache()). Fastest. |
| `MEMORY_AND_DISK` | ✅ | ✅ (spill) | No | No | Large datasets that don't fit in RAM |
| `DISK_ONLY` | ❌ | ✅ | Yes | No | Very large datasets, save RAM |
| `MEMORY_ONLY_2` | ✅ | ❌ | No | 2× | Critical data needing fault tolerance |

### Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Small table (e.g., lookup/dim table) → broadcast to all executors
small_df = spark.createDataFrame([(1, "Credit"), (2, "Cash")], ["id", "name"])

# Without broadcast: shuffle both tables
result = big_df.join(small_df, big_df["payment_type"] == small_df["id"])

# With broadcast: send small_df to every executor (no shuffle of big_df)
result = big_df.join(broadcast(small_df), big_df["payment_type"] == small_df["id"])
```

### Partition Tuning

```python
# Check current partitions
df.rdd.getNumPartitions()    # e.g., 2 (from HDFS block count)

# Increase partitions (triggers shuffle)
df = df.repartition(8)       # good for heavy transformations

# Decrease partitions (no shuffle — coalesces locally)
df = df.coalesce(2)           # good before writing output

# Partition by column (co-locate related rows)
df = df.repartition(4, "District")
```

### Spark Web UI — Key Pages

| Tab | What It Shows | What to Look For |
|---|---|---|
| **Jobs** | All submitted jobs | Failed jobs (red), duration |
| **Stages** | Stages within each job | Shuffle Read/Write bytes, task count |
| **Tasks** | Individual tasks per stage | Stragglers (outlier durations), locality level |
| **Storage** | Cached RDDs/DataFrames | % cached, memory used |
| **Executors** | Executor-level metrics | Memory usage, GC time, shuffle spill |

---

## Lab: Spark Performance Tuning (`labs/01_spark_tuning_lab/`)

Students experiment with caching and partitioning on the cluster:

1. **Baseline**: Run a multi-action pipeline **without** caching — record times for 3 actions
2. **With caching**: Add `.cache()` after loading — re-run, record times
3. **Partition experiment**: `repartition(1)` vs `repartition(4)` vs `repartition(8)` — compare `groupBy` performance
4. **Broadcast join**: Join crimes with a small lookup table; compare with and without `broadcast()`
5. **Web UI analysis**: Screenshot the Storage tab (cached data), Stages tab (shuffle bytes), Executors tab (memory)

**Expected Output:** Lab report table with timing comparisons and Web UI screenshots

---

## Dataset

All labs use the Chicago Crimes dataset already on HDFS:

```
hdfs:///data/chicago_crimes.csv          ← full dataset
```

Schema reminder:

| Column           | Type    | Description                          |
|------------------|---------|--------------------------------------|
| ID               | int     | Unique crime ID                      |
| Date             | string  | Datetime of incident                 |
| Block            | string  | Anonymized address block             |
| Primary Type     | string  | Crime category (THEFT, BATTERY, ...) |
| District         | int     | Police district number               |
| Arrest           | boolean | Whether an arrest was made           |
| Domestic         | boolean | Whether domestic-related             |
| Year             | int     | Year of incident                     |

---

## Connection to Previous & Next Weeks

```
Week 2: HDFS          → Where data lives (blocks on DataNodes)
Week 3-4: MapReduce   → First distributed processing model (disk-based)
Week 5: Hive          → SQL-on-Hadoop (HiveQL, schema on read)
Week 6: YARN          → Cluster resource manager (runs all our jobs)
Week 7: Spark Core    → In-memory processing (RDDs, DataFrames, SQL)
Week 8: THIS WEEK     → Spark on Cluster + Performance Tuning
Week 9: Spark MLlib   → Machine Learning at Scale
Week 10: Kafka + Streaming → Real-time data processing
```

---

## Folder Structure

```
week08/
├── README.md                              ← this file
├── slides/
│   ├── current/
│   │   └── SE446_W08A_spark_cluster.tex
│   ├── notes/
│   │   ├── SE446_W08A_spark_cluster_notes_koubaa.tex
│   │   └── SE446_W08A_spark_cluster_notes_walkthrough.tex
│   ├── spark-ui/                          ← Spark Web UI screenshots
│   └── v1/
│       └── SE446_W08A_spark_cluster.tex   ← previous version
├── labs/
│   └── 01_spark_tuning_lab/
│       ├── lab_instructions.md
│       ├── quick_start_spark_yarn_ui.md
│       └── spark_cluster_quick_start.ipynb
```

---

## Additional Resources

- 📖 [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- 📖 [Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- 📖 [Spark Web UI Deep Dive](https://spark.apache.org/docs/latest/web-ui.html)
- 📖 [Learning Spark, 2nd Ed — Chapter 7: Optimizations](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
