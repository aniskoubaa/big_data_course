# Week 11: Kafka & Spark Streaming -- Hands-On Labs

## Overview

Week 11 is a **lab-only week** dedicated to hands-on practice with the real-time processing technologies introduced in Week 10. **Session 11A** (Monday) covers Apache Kafka fundamentals -- creating topics, producing and consuming messages with CLI and Python. **Session 11B** (Wednesday) covers Spark Structured Streaming -- reading from Kafka, applying transformations, and writing results with different output modes.

**Milestone M4** (Streaming Pipeline) is due at the end of this week.

---

## Session 11A: Kafka Fundamentals Lab (Monday)

### Learning Objectives
1. Create Kafka topics with multiple partitions and verify with `--describe`
2. Produce and consume messages using `kafka-console-producer` and `kafka-console-consumer`
3. Write a Python Kafka producer using `confluent-kafka` to publish JSON messages
4. Write a Python Kafka consumer and observe consumer group behavior
5. Experiment with message keys and partition assignment strategies

### Key Commands

```bash
# Create a topic with 3 partitions
kafka-topics.sh --create --topic crimes-stream \
    --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1

# Produce messages (interactive)
kafka-console-producer.sh --topic crimes-stream \
    --bootstrap-server localhost:9092

# Consume messages from beginning
kafka-console-consumer.sh --topic crimes-stream \
    --bootstrap-server localhost:9092 --from-beginning

# Consume in a consumer group
kafka-console-consumer.sh --topic crimes-stream \
    --bootstrap-server localhost:9092 --group my-group
```

### Materials
- Lab: `labs/01_kafka_fundamentals_lab/lab_instructions.md`

---

## Session 11B: Spark Structured Streaming Lab (Wednesday)

### Learning Objectives
1. Read from a Kafka topic using `spark.readStream`
2. Parse JSON messages from Kafka into structured DataFrames
3. Apply transformations on streaming DataFrames (filter, groupBy, aggregations)
4. Write results using output modes: `append`, `update`, `complete`
5. Implement tumbling and sliding time windows for real-time aggregation
6. Build an end-to-end pipeline: Kafka producer --> Kafka topic --> Spark consumer --> console/file sink

### Key Patterns

```python
# Read from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crimes-stream") \
    .load()

# Parse JSON values
from pyspark.sql.functions import from_json, col, window
parsed = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Tumbling window aggregation
counts = parsed.groupBy(
    window("timestamp", "5 minutes"),
    "crime_type"
).count()

# Write results
query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
```

### Materials
- Lab: `labs/02_spark_streaming_lab/lab_instructions.md`

---

## Prerequisites

- Week 10 lectures on Kafka architecture and Structured Streaming concepts
- SSH access to the cluster (Kafka broker at port 9092)
- PySpark with Kafka connector: `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`

---

## Milestone M4 -- Streaming Pipeline

**Due:** End of Week 11 (Sunday 23:59)

### Deliverables
1. **Kafka Producer**: Python script that reads Chicago Crimes CSV and publishes rows as JSON to a Kafka topic
2. **Spark Consumer**: PySpark script that reads from Kafka, applies at least 2 transformations, and writes results
3. **Windowed Aggregation**: At least one tumbling or sliding window analysis (e.g., crime count per 5-minute window)
4. **Written Report**: 1-page description of architecture, design decisions, and sample output

### Submission
- Scripts: `M4_Streaming_<StudentID>.py` (or `.ipynb`)
- Report: `M4_Report_<StudentID>.pdf`
- Submit via Moodle

---

## Connection to Previous Weeks

```
Week 7: Spark Core        --> RDDs, DataFrames, Spark SQL
Week 8: Spark Cluster     --> spark-submit, caching, partitions
Week 9: Spark MLlib       --> ML pipelines (batch)
Week 10: Kafka + Streaming --> Lectures (architecture, concepts)
Week 11: THIS WEEK        --> Hands-on labs (produce, consume, stream)
```

---

## Additional Resources

- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Confluent Kafka Python Client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/)

---

## Folder Structure

```
week11/
├── README.md                              <-- this file
├── labs/
│   ├── 01_kafka_fundamentals_lab/
│   │   └── lab_instructions.md
│   └── 02_spark_streaming_lab/
│       └── lab_instructions.md
```
