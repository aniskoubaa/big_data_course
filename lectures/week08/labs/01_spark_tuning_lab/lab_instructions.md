# Lab 08-1: Spark Performance Tuning

**Course:** SE446 — Big Data Systems  
**Week:** 08 | **Estimated Duration:** 90 minutes  

---

## Overview

In this lab you will tune a real Spark application running on a YARN cluster and **measure the actual performance impact** of three fundamental optimization techniques: **caching**, **partition tuning**, and **broadcast joins**.

By the end of this lab you will be able to:
- Explain why Spark re-reads data from HDFS on every action, and how caching prevents that.
- Choose an appropriate partition count for a given cluster size.
- Distinguish between a shuffle join and a broadcast join, and know when to use each.
- Read the Spark Web UI to diagnose bottlenecks.

---

## Background Concepts

> Read this section carefully before writing any code. Understanding *why* matters as much as *how*.

### 1. Lazy Evaluation and the Cost of Actions

Spark builds a **DAG (Directed Acyclic Graph)** of transformations (e.g., `filter`, `groupBy`, `select`) but does **nothing** until an **action** is called (e.g., `count`, `collect`, `show`). Each action triggers a full re-execution of the DAG from the source — which means reading from HDFS again and again unless you explicitly tell Spark to remember the data.

```
read CSV → filter → groupBy → count   ← action triggers full re-run
```

**Key insight:** If you call three actions on the same DataFrame without caching, Spark reads the CSV file from HDFS *three times*.

### 2. Caching — Keep Data in Memory

`.cache()` tells Spark to store the DataFrame in executor memory after its first computation. Subsequent actions reuse that in-memory copy instead of going back to disk.

```
read CSV → cache in memory
                ↓
     Action 1: count()       ← reads from memory (fast)
     Action 2: groupBy()     ← reads from memory (fast)
     Action 3: filter()      ← reads from memory (fast)
```

**When to cache:** Cache a DataFrame when you will use it more than once. Don't cache everything — memory is shared across tasks and unnecessary caching wastes resources.

### 3. Partitions — The Unit of Parallelism

Spark splits data into **partitions**. Each partition is processed by one task running on one core. The number of partitions therefore controls how much work can happen in parallel.

- **Too few partitions:** Cores sit idle. Some tasks handle huge chunks of data.
- **Too many partitions:** Scheduling overhead dominates. Each task does almost no real work.
- **Rule of thumb:** 2–4 partitions per available core is a good starting point.

Our cluster has **4 total cores** (2 executors × 2 cores each). So the sweet spot is somewhere between 4 and 16 partitions.

### 4. Joins and the Shuffle Problem

When you join two large DataFrames, Spark must ensure that rows with the same key end up on the same machine. This requires moving data across the network — called a **shuffle**. Shuffles are expensive: they generate disk I/O, network traffic, and serialisation overhead.

**Broadcast join** solves this for the common case where one table is small: Spark sends (broadcasts) a full copy of the small table to every executor. Each executor then performs the join locally without any network shuffle.

```
Normal join:    big_table ←→ shuffle ←→ other_table   (slow — network traffic)
Broadcast join: each executor gets a full copy of small_table (fast — no shuffle)
```

**Rule of thumb:** Use broadcast join when one side is smaller than ~10 MB (configurable via `spark.sql.autoBroadcastJoinThreshold`).

---

## Prerequisites

- SSH access to the cluster master node
- Dataset available at `hdfs:///data/chicago_crimes.csv`
- PySpark shell or `spark-submit` working on YARN

---

## Setup

```bash
# SSH to master node
ssh hadoop@134.209.172.50

# Start PySpark on YARN
pyspark --master yarn --deploy-mode client \
    --num-executors 2 --executor-memory 1g --executor-cores 2
```

Verify your session is connected to YARN:
```python
spark.sparkContext.master   # should print 'yarn'
```

---

## Experiment 1: Caching Impact

**Goal:** Quantify how much time is wasted re-reading data from HDFS when caching is not used.

### Step 1: Without Caching

Run the following and record the elapsed time for each action:

```python
import time

df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)

# Action 1: count total rows
t1 = time.time()
df.count()
print(f"Action 1 (count): {time.time() - t1:.2f}s")

# Action 2: group crimes by type
t2 = time.time()
df.groupBy("Primary Type").count().collect()
print(f"Action 2 (groupBy): {time.time() - t2:.2f}s")

# Action 3: count arrests
t3 = time.time()
df.filter(df["Arrest"] == True).count()
print(f"Action 3 (filter): {time.time() - t3:.2f}s")
```

### Step 2: With Caching

```python
df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)
df.cache()

# Trigger the cache (first action is still a full read)
df.count()

# Now repeat Actions 2 and 3 and record times
t2 = time.time()
df.groupBy("Primary Type").count().collect()
print(f"Action 2 cached (groupBy): {time.time() - t2:.2f}s")

t3 = time.time()
df.filter(df["Arrest"] == True).count()
print(f"Action 3 cached (filter): {time.time() - t3:.2f}s")

# Release memory when done
df.unpersist()
```

### Step 3: Record Results

| Action | Without Cache (s) | With Cache (s) | Speedup (×) |
|--------|:-----------------:|:--------------:|:-----------:|
| count() | | | |
| groupBy("Primary Type").count() | | | |
| filter(Arrest==True).count() | | | |
| **Total** | | | |

Calculate Speedup as: `Without_Cache_time / With_Cache_time`.

### Questions — Experiment 1

> Answer these questions in your submission report.

1. Why is Action 1 (`count`) roughly the same time with and without caching?
2. After calling `.cache()`, Spark does not immediately load data into memory. Which action actually triggers the caching, and why?
3. If the dataset were 100 GB and your cluster had only 4 GB of executor memory per node, what would happen when you call `.cache()`? What Spark storage level handles this gracefully?
4. You have a pipeline that reads a CSV, applies 5 different filters, and writes 5 separate output files. Sketch the DAG with and without caching. How many times does Spark read the CSV in each case?

---

## Experiment 2: Partition Tuning

**Goal:** Find the optimal number of partitions for our cluster by measuring how partition count affects job duration.

### Step 1: Check the Default Partition Count

```python
df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)
print(f"Default partitions: {df.rdd.getNumPartitions()}")
```

The default is usually determined by the HDFS block size (128 MB). Note whether this matches your expectation for the file size.

### Step 2: Benchmark Different Partition Counts

```python
import time

df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)
df.cache()
df.count()  # warm up the cache

results = {}
for num_parts in [1, 2, 4, 8, 16, 32]:
    df_repart = df.repartition(num_parts)
    t = time.time()
    df_repart.groupBy("District").count().collect()
    elapsed = time.time() - t
    results[num_parts] = elapsed
    print(f"Partitions={num_parts:>3}: {elapsed:.2f}s")

df.unpersist()
```

### Step 3: Record Results

| Partitions | groupBy Time (s) | CPU Utilisation | Notes |
|:----------:|:----------------:|:---------------:|-------|
| 1 | | Low | |
| 2 | | | |
| 4 | | | |
| 8 | | | |
| 16 | | | |
| 32 | | High overhead | |

### Questions — Experiment 2

1. Our cluster has 2 executors × 2 cores = **4 total cores**. Which partition count gave the best performance? Is it exactly 4, or slightly higher? Why might 8 outperform 4?
2. What happened at 1 partition? Explain why parallelism drops to zero even though multiple cores are available.
3. At 32 partitions, what overhead is introduced? Check the Spark UI Stages tab — how many tasks were launched?
4. `repartition(n)` causes a full shuffle. `coalesce(n)` reduces partitions without a shuffle but cannot increase them. When would you prefer `coalesce` over `repartition` in a production ETL pipeline?
5. *(Challenge)* Check the value of `spark.sql.shuffle.partitions` in your session. What is it? What does it control, and why might the default of 200 be a bad choice for a small cluster?

---

## Experiment 3: Broadcast Join

**Goal:** Compare shuffle join vs broadcast join execution time and examine their query plans.

### Step 1: Create a Small Lookup Table

```python
district_names = spark.createDataFrame([
    (1,  "Central"),        (2,  "Wentworth"),    (3,  "Grand Crossing"),
    (4,  "South Chicago"),  (5,  "Calumet"),       (6,  "Gresham"),
    (7,  "Englewood"),      (8,  "Chicago Lawn"),  (9,  "Deering"),
    (10, "Ogden"),          (11, "Harrison")
], ["DistrictID", "DistrictName"])

# This table has only 11 rows — it is tiny compared to the crimes dataset
print(f"Lookup table size: {district_names.count()} rows")
```

### Step 2: Measure Normal (Shuffle) Join

```python
import time

crimes = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)

t = time.time()
result = crimes.join(district_names, crimes["District"] == district_names["DistrictID"])
count_normal = result.count()
elapsed_normal = time.time() - t
print(f"Normal join: {elapsed_normal:.2f}s  — {count_normal} rows matched")
```

### Step 3: Measure Broadcast Join

```python
from pyspark.sql.functions import broadcast

t = time.time()
result = crimes.join(broadcast(district_names), crimes["District"] == district_names["DistrictID"])
count_broadcast = result.count()
elapsed_broadcast = time.time() - t
print(f"Broadcast join: {elapsed_broadcast:.2f}s  — {count_broadcast} rows matched")

print(f"\nSpeedup: {elapsed_normal / elapsed_broadcast:.2f}×")
```

### Step 4: Inspect Execution Plans

Reading the query plan tells you *what Spark actually does internally*.

```python
print("=== NORMAL JOIN PLAN ===")
crimes.join(district_names, crimes["District"] == district_names["DistrictID"]).explain()

print("\n=== BROADCAST JOIN PLAN ===")
crimes.join(broadcast(district_names), crimes["District"] == district_names["DistrictID"]).explain()
```

Look for the keywords **`SortMergeJoin`** (normal) and **`BroadcastHashJoin`** (broadcast) in the output.

### Step 5: Record Results

| Join Type | Execution Time (s) | Join Strategy (from plan) | Shuffle I/O? |
|-----------|:-----------------:|:-------------------------:|:------------:|
| Normal join | | | Yes / No |
| Broadcast join | | | Yes / No |
| **Speedup** | | | |

### Questions — Experiment 3

1. In the normal join plan, can you find a `Exchange` operator? What does it represent, and why does it appear?
2. Spark has a configuration `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). What does it do? Run `spark.conf.get("spark.sql.autoBroadcastJoinThreshold")` and check the value. Given our `district_names` table is 11 rows, would Spark have broadcast it automatically?
3. If both tables were large (e.g., 50 GB each), would a broadcast join be possible? What strategy would Spark use instead?
4. *(Challenge)* What happens if you broadcast the *large* crimes dataset instead of the small lookup table? Predict the outcome before trying it. (Hint: think about memory.)
5. *(Design question)* You are building a real-time pipeline that joins a 500 GB transaction log with a 5 MB country-code lookup table, updated daily. How would you structure this join for best performance?

---

## Experiment 4: Spark Web UI Analysis

The Spark Web UI at `http://master:4040` is your primary observability tool. It shows you exactly what Spark did, how long each stage took, and where the bottlenecks are.

While running the experiments above, collect the following metrics:

| Metric | Value |
|--------|-------|
| Total jobs submitted (Jobs tab) | |
| Stages in the groupBy job | |
| Shuffle Read bytes (Stages tab) | |
| Shuffle Write bytes (Stages tab) | |
| Cached DataFrame size (Storage tab) | |
| Task locality: % NODE_LOCAL | |
| Executor memory used | |

**Take screenshots of:**
1. **Jobs tab** — showing all completed jobs and their durations.
2. **Storage tab** — showing the cached DataFrame and its memory footprint.
3. **Stages tab** — for the `groupBy` job, specifically showing shuffle read/write metrics.
4. **SQL/DataFrame tab** — showing the query plan for the broadcast join (visually).

**Web UI Questions:**

1. On the Jobs tab, click into the `groupBy` job. How many stages does it have? What does each stage correspond to in the computation?
2. On the Storage tab, what fraction of the DataFrame is cached in memory vs spilled to disk? What does a high disk-spill fraction tell you?
3. Compare shuffle read/write bytes between the normal join and the broadcast join jobs. What is the difference? Why does the broadcast join produce near-zero shuffle bytes?
4. What is "Task Locality"? Why does `NODE_LOCAL` locality lead to faster execution than `ANY` locality?

---

## Reflection Questions

Answer these after completing all experiments. They require you to connect ideas across the entire lab.

1. You have a Spark job that reads a 10 GB file, applies 4 transformations, and runs 6 different aggregations. Without caching, how many times does Spark read the file? With caching at the right point, how many times? Where exactly in the DAG would you place the `.cache()` call?

2. A colleague says: *"I always set `spark.sql.shuffle.partitions=200` — it's the Spark default so it must be safe."* Your cluster has 5 nodes, 4 cores each, processing 500 MB of data. Critique this statement and suggest a better value with reasoning.

3. You are joining a 200 GB log table with a 50 MB product catalog. The product catalog fits in memory per executor, but exceeds the default `autoBroadcastJoinThreshold`. What are your two options? What are the trade-offs of each?

4. Describe a real-world scenario (outside of this dataset) where all three techniques from this lab — caching, partition tuning, and broadcast joins — would be applied together.

---

## Deliverables

Submit a report (PDF or Markdown) containing:

1. Completed timing and metrics tables for all 4 experiments.
2. Written answers to **all** questions (Experiments 1–4 + Reflection).
3. Four Spark Web UI screenshots, each labelled and annotated.
4. Commit your PySpark notebook (`.ipynb` or `.py`) to your team's GitHub repo and include the link.

---

## Grading

| Component | Points |
|-----------|:------:|
| Experiment 1: Caching — results + 4 questions answered | 25 |
| Experiment 2: Partition tuning — results + 5 questions answered | 25 |
| Experiment 3: Broadcast join — results + 5 questions answered | 25 |
| Experiment 4: Web UI — metrics table + 4 questions answered | 15 |
| Reflection questions (4 questions) | 10 |
| **Total** | **100** |
