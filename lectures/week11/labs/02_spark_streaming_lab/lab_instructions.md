# Lab 09-2: Spark Structured Streaming with Kafka

**Course:** SE446 — Big Data Systems  
**Week:** 09 | **Estimated Duration:** 90 minutes

---

## Overview

In this lab you will build a **real-time streaming pipeline**: a Python producer simulates live crime events into Kafka, and a Spark Structured Streaming job reads, transforms, and aggregates the data continuously. You will implement time-windowed aggregations and observe micro-batch execution — all using the same DataFrame API you learned in Week 7–8.

By the end of this lab you will be able to:
- Connect Spark Structured Streaming to a Kafka topic.
- Parse JSON messages from Kafka into structured DataFrames.
- Compute real-time aggregations using `groupBy`, tumbling windows, and sliding windows.
- Choose the correct output mode (append, update, complete) for each query.
- Explain watermarks and why they are needed for late-arriving data.

---

## Background Concepts

### 1. Batch vs. Streaming — Same Code, Different Semantics

The core idea of Structured Streaming is that you write the **exact same code** as a batch job:

```python
# BATCH (read once, process once):
df = spark.read.format("kafka").load()
result = df.groupBy("type").count()
result.write.format("parquet").save("/output")

# STREAMING (read continuously, process repeatedly):
df = spark.readStream.format("kafka").load()
result = df.groupBy("type").count()
result.writeStream.format("console").start()
```

Spark handles the complexity: it keeps track of what's new, processes only the new rows, and incrementally updates the results. You don't write a loop — Spark does it internally via **micro-batches**.

### 2. The Micro-Batch Model

Every few seconds (configurable), Spark:
1. Reads new messages from Kafka since the last micro-batch.
2. Appends them to the "infinite input table."
3. Re-runs your query (incrementally — not from scratch).
4. Writes the updated result.

```
Time 0s:   Read 50 msgs → groupBy → write counts to console
Time 5s:   Read 30 msgs → update groupBy counts → write updated counts
Time 10s:  Read 45 msgs → update again → write
...
```

### 3. Output Modes — What Gets Written

| Mode | Behaviour | When to Use |
|------|-----------|-------------|
| **Complete** | Writes the **entire** result table every micro-batch | Small aggregations (few groups) |
| **Update** | Writes only **rows that changed** since last micro-batch | Dashboards, databases |
| **Append** | Writes only **new rows** (never updates old) | Logging, HDFS archival |

**Critical rule:** If your query has a `groupBy`, you **cannot** use append mode (because group counts keep changing). Use complete or update instead.

### 4. Time Windows — Grouping Events by Time

In batch, you group by a column like `district`. In streaming, you often group by **time** — "how many crimes in the last 5 minutes?"

**Tumbling window** (non-overlapping):
```
|── 5 min ──|── 5 min ──|── 5 min ──|
    10 events    7 events    12 events
```

**Sliding window** (overlapping):
```
|────── 10 min ──────|
      |────── 10 min ──────|
            |────── 10 min ──────|
   ← 2 min slide →
```

Sliding windows produce more results but give smoother trends — useful for dashboards.

### 5. Watermarks — Handling Late Data

Real-world data arrives late. A crime event timestamped 14:03 might arrive at Kafka at 14:07. Without a watermark, Spark must keep **all** window state forever (unbounded memory). A watermark says: "I accept events up to X minutes late. After that, I can discard old state."

```python
.withWatermark("event_time", "10 minutes")
```

This means: if the latest event is at 14:20, Spark can drop state for windows ending before 14:10.

---

## Prerequisites

- SSH access to the cluster: `ssh <username>@134.209.172.50`
- Kafka running with topic `lab-crimes` created (from Lab 09-1)
- PySpark with Kafka connector available

---

## Setup

### Terminal 1: Start PySpark with Kafka Support

```bash
ssh <your_username>@134.209.172.50

pyspark --master yarn --deploy-mode client \
    --num-executors 2 --executor-memory 1g --executor-cores 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.sql.shuffle.partitions=4
```

The `--packages` flag downloads the Kafka connector at startup. This may take a minute the first time.

### Terminal 2: Prepare the Crime Producer

Create a file `crime_producer.py`:

```python
#!/usr/bin/env python3
"""Simulates live crime events by producing to Kafka at a steady rate."""

from confluent_kafka import Producer
import json, time, random
from datetime import datetime

producer = Producer({'bootstrap.servers': 'localhost:9092'})

CRIME_TYPES = ["THEFT", "BATTERY", "ROBBERY", "ASSAULT", "BURGLARY",
               "NARCOTICS", "CRIMINAL DAMAGE", "MOTOR VEHICLE THEFT"]
DISTRICTS = list(range(1, 12))

def generate_crime():
    return {
        "id": random.randint(10000, 99999),
        "type": random.choice(CRIME_TYPES),
        "district": random.choice(DISTRICTS),
        "hour": datetime.now().hour,
        "arrest": random.random() < 0.27,  # ~27% arrest rate
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("Producing crime events (Ctrl+C to stop)...")
count = 0
try:
    while True:
        crime = generate_crime()
        producer.produce(
            topic="lab-crimes",
            key=str(crime["district"]).encode("utf-8"),
            value=json.dumps(crime).encode("utf-8")
        )
        count += 1
        if count % 10 == 0:
            print(f"Produced {count} events | Latest: {crime['type']} in district {crime['district']}")
        producer.poll(0)
        time.sleep(0.5)  # 2 events per second
except KeyboardInterrupt:
    producer.flush()
    print(f"\nDone. Total produced: {count}")
```

Run it: `python3 crime_producer.py`

Leave this running throughout the lab — it provides the continuous data stream.

---

## Part 1: Reading from Kafka

**Goal:** Connect Spark to Kafka and parse JSON messages into a structured DataFrame.

### Step 1: Create the Stream

In PySpark (Terminal 1):

```python
# Read from Kafka as a stream
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lab-crimes") \
    .option("startingOffsets", "latest") \
    .load()

# Inspect the raw schema — Kafka gives us key, value, topic, partition, offset, timestamp
raw_stream.printSchema()
```

Record the schema. Note that `key` and `value` are **binary** (byte arrays), not strings.

### Step 2: Parse JSON

```python
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType

# Define the JSON schema matching our producer output
crime_schema = StructType() \
    .add("id", IntegerType()) \
    .add("type", StringType()) \
    .add("district", IntegerType()) \
    .add("hour", IntegerType()) \
    .add("arrest", BooleanType()) \
    .add("timestamp", StringType())

# Parse: bytes → string → JSON → individual columns
crimes_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), crime_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))
```

### Step 3: Debug — Write to Console

```python
# Write the parsed stream to the console (append mode — just print new rows)
debug_query = crimes_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .option("truncate", False) \
    .start()

# Watch for ~30 seconds, then stop
import time
time.sleep(30)
debug_query.stop()
```

You should see crime events printed every 5 seconds. Each micro-batch shows the new rows that arrived since the last batch.

### Questions — Part 1

1. The raw Kafka DataFrame has columns: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`. What is the difference between Kafka's `timestamp` (message metadata) and our `event_time` (inside the JSON payload)?
2. Why did we use `CAST(value AS STRING)` before `from_json`? What type is `value` originally?
3. `startingOffsets` was set to `"latest"`. If you change it to `"earliest"`, what happens when you start the streaming query? When would you use each?
4. If the JSON schema in `crime_schema` doesn't match the actual producer output (e.g., wrong column name), what happens? Does the query crash, or does it produce `null` values?

---

## Part 2: Real-Time Aggregations

**Goal:** Compute running counts and statistics on the live stream.

### Experiment 1: Crime Count by Type (Complete Mode)

```python
type_counts = crimes_df.groupBy("type").count()

query_types = type_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

# Run for 60 seconds
time.sleep(60)
query_types.stop()
```

Every 10 seconds, the **entire** count table is printed. Observe how the counts grow.

### Experiment 2: Crime Count by District (Update Mode)

```python
district_counts = crimes_df.groupBy("district").count()

query_districts = district_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

time.sleep(60)
query_districts.stop()
```

With **update mode**, only rows whose counts changed are printed. Compare the output volume with complete mode.

### Experiment 3: Running Arrest Rate

```python
from pyspark.sql.functions import sum as spark_sum, count, round as spark_round

arrest_rate = crimes_df.groupBy("type").agg(
    count("*").alias("total"),
    spark_sum(col("arrest").cast("integer")).alias("arrests"),
    spark_round(spark_sum(col("arrest").cast("integer")) / count("*") * 100, 2).alias("arrest_rate_%")
)

query_arrest = arrest_rate.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

time.sleep(60)
query_arrest.stop()
```

### Record Results

After 60 seconds, record the final state:

| Crime Type | Total Events | Arrests | Arrest Rate (%) |
|-----------|:-----------:|:-------:|:---------------:|
| THEFT | | | |
| BATTERY | | | |
| ROBBERY | | | |
| ASSAULT | | | |
| BURGLARY | | | |
| NARCOTICS | | | |
| Other... | | | |

### Questions — Part 2

1. Why does `groupBy("type").count()` require **complete** or **update** mode? What goes wrong if you try **append** mode? (Try it and read the error message.)
2. In **complete** mode, the entire table is re-written every micro-batch. If you have 1 million distinct crime types, how much data is written per micro-batch? When does complete mode become impractical?
3. In **update** mode, only changed rows are written. In our scenario (every crime updates some type count), are there ever micro-batches where nothing is written? When would update mode write fewer rows than complete mode?
4. The arrest rate starts volatile (fluctuating with small sample sizes) and stabilizes over time. This is the **law of large numbers** in action. After how many events did the rates stabilize for you?
5. *(Challenge)* Can you combine a streaming aggregation with an `orderBy` to show the top 3 crime types by count? Try it — what error do you get, and why?

---

## Part 3: Time-Windowed Aggregations

**Goal:** Count crimes in fixed time windows — the most common real-time analytics pattern.

### Step 1: Tumbling Window (Non-Overlapping)

```python
from pyspark.sql.functions import window

# Count crimes per type in 1-minute tumbling windows
tumbling_counts = crimes_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window("event_time", "1 minute"),  # 1-minute tumbling window
        "type"
    ) \
    .count()

query_tumbling = tumbling_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

# Run for 3+ minutes to see multiple windows complete
time.sleep(180)
query_tumbling.stop()
```

Observe the output carefully. The `window` column shows `[start, end)` times. When a 1-minute window closes, the final counts appear.

### Step 2: Sliding Window (Overlapping)

```python
# Count crimes per district in 2-minute windows, sliding every 30 seconds
sliding_counts = crimes_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window("event_time", "2 minutes", "30 seconds"),  # window=2min, slide=30s
        "district"
    ) \
    .count()

query_sliding = sliding_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("truncate", False) \
    .start()

time.sleep(180)
query_sliding.stop()
```

### Step 3: Record Window Results

After 3 minutes, record a few windows:

| Window Start | Window End | Crime Type / District | Count |
|-------------|-----------|----------------------|:-----:|
| | | | |
| | | | |
| | | | |
| | | | |

### Questions — Part 3

1. In the tumbling window output, a window `[14:00, 14:01)` closes at 14:01. But due to the 2-minute watermark, Spark waits until 14:03 to finalize it. Why? What would happen to events timestamped 14:00:45 that arrive at 14:01:30?

2. The sliding window with `window=2min, slide=30s` produces **4 overlapping windows** per minute. How many windows will contain a single event timestamped 14:01:15? List the window boundaries.

3. We used `withWatermark("event_time", "2 minutes")`. What happens if you remove the watermark entirely? (Hint: think about memory.) What happens if you set it to "0 seconds"?

4. A tumbling window of 1 minute and a trigger of 10 seconds means Spark checks for new data every 10 seconds, but windows close every 60 seconds. During the first 50 seconds of a window, what does Spark write in **update** mode?

5. *(Design question)* You need to build a real-time dashboard showing "crimes in the last 15 minutes, updated every minute." Should you use a tumbling or sliding window? What are the parameters?

---

## Part 4: Writing to Memory Table (Interactive Queries)

**Goal:** Write stream results to an in-memory table for interactive SQL queries — useful for dashboards and exploration.

### Step 1: Create a Memory Sink

```python
crime_summary = crimes_df.groupBy("type", "district").count()

query_memory = crime_summary.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("crime_dashboard") \
    .trigger(processingTime="10 seconds") \
    .start()

# Give it time to accumulate data
time.sleep(30)
```

### Step 2: Query the In-Memory Table with SQL

```python
# Now you can query this table interactively with regular Spark SQL
spark.sql("SELECT * FROM crime_dashboard ORDER BY count DESC LIMIT 10").show()
spark.sql("SELECT type, SUM(count) as total FROM crime_dashboard GROUP BY type ORDER BY total DESC").show()
spark.sql("SELECT district, SUM(count) as total FROM crime_dashboard GROUP BY district ORDER BY total DESC").show()
```

Run these queries multiple times over 1–2 minutes. The numbers change because the stream keeps updating the `crime_dashboard` table.

### Step 3: Monitor Query Progress

```python
# Check micro-batch statistics
print(query_memory.status)
print(query_memory.lastProgress)

# Key fields in lastProgress:
# - numInputRows: how many Kafka messages were processed in this batch
# - processedRowsPerSecond: throughput
# - durationMs: time spent in each phase (trigger, getBatch, query)

query_memory.stop()
```

### Questions — Part 4

1. The `memory` sink stores the entire result table in Spark driver memory. Why is this only suitable for small result sets? What happens if the result has millions of rows?
2. You ran `spark.sql()` queries on `crime_dashboard` while the stream was running. Are these results guaranteed to be from the latest micro-batch, or could they be stale?
3. `lastProgress` reports `processedRowsPerSecond`. If this number is **lower** than your producer rate (2 events/sec), what does that indicate? What would you do to fix it?
4. In what production scenario would you use `format("memory")` instead of `format("kafka")` or `format("parquet")`?

---

## Part 5: End-to-End Challenge

**Goal:** Build a complete streaming analytics pipeline on your own.

### Challenge Brief

Build a pipeline that:
1. Reads from the `lab-crimes` Kafka topic.
2. Computes, in 2-minute tumbling windows with a 3-minute watermark:
   - Total crimes per window
   - Arrest count per window
   - Arrest rate (%) per window
3. Writes results to both console AND an in-memory table called `arrest_trends`.
4. After 5 minutes, query `arrest_trends` to find the window with the highest arrest rate.

### Starter Code

```python
# YOUR CODE HERE
# 1. Read stream from Kafka (same as Part 1)
# 2. Parse JSON
# 3. Use .withWatermark() and window() to create 2-minute tumbling windows
# 4. Aggregate: count(*), sum(arrest), arrest_rate
# 5. writeStream to console (update mode) AND memory (complete mode)
# 6. After 5 min: spark.sql("SELECT * FROM arrest_trends ORDER BY ...")
```

---

## Reflection Questions

Answer these after completing all parts.

1. You processed ~2 events/second in this lab. In production, a Kafka topic might receive 100,000 events/second. What Spark configuration changes would you make to handle this throughput? Consider: executors, partitions, trigger interval, and memory.

2. We used `format("console")` for output. In a real system, you'd write to a database or another Kafka topic. Explain the trade-offs between writing to: (a) Kafka, (b) PostgreSQL via `foreachBatch`, (c) Parquet files on HDFS.

3. Your streaming pipeline has been running for 3 hours. The `crime_dashboard` memory table has accumulated 500 MB of aggregation state. The Spark driver has 2 GB of memory. What happens next? How do watermarks prevent this?

4. Compare and contrast: a batch Spark job that runs every 5 minutes processing the last 5 minutes of data from HDFS, vs. a Structured Streaming job with a 5-second trigger reading from Kafka. What are the advantages of each approach?

5. *(Ethics)* A streaming pipeline predicts "high crime probability" zones in real-time and dispatches police accordingly. What are the risks of using such a system? How could feedback loops amplify existing biases in the training data?

---

## Deliverables

Submit a report (PDF or Markdown) containing:

1. Console output from Part 1 (parsed crime events).
2. Aggregation results from Part 2 (arrest rate table).
3. Windowed output from Part 3 (tumbling + sliding, at least 3 complete windows each).
4. SQL query results from Part 4 (top 10 crime summary).
5. Challenge pipeline code and results (Part 5).
6. Written answers to **all** questions (Parts 1–4 + Reflection).
7. Commit all Python scripts to your team's GitHub repo.

---

## Grading

| Component | Points |
|-----------|:------:|
| Part 1: Kafka → Spark connection + parsing + 4 questions | 15 |
| Part 2: Real-time aggregations + 5 questions | 20 |
| Part 3: Time windows (tumbling + sliding) + 5 questions | 25 |
| Part 4: Memory sink + interactive queries + 4 questions | 10 |
| Part 5: End-to-end challenge pipeline | 15 |
| Reflection questions (5 questions) | 15 |
| **Total** | **100** |
