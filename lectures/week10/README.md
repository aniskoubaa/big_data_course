# Week 10: Apache Kafka & Spark Structured Streaming

## Overview

Week 10 introduces **real-time data processing** — the final major paradigm in the Big Data stack. **Session 10A** (Monday) covers Apache Kafka as the distributed messaging backbone: topics, partitions, producers, consumers, and consumer groups. **Session 10B** (Wednesday) introduces Spark Structured Streaming, which reads from Kafka and processes infinite data streams using the familiar DataFrame API. **Hands-on labs** for both topics are in **Week 11**.

---

## Session 10A: Apache Kafka — Distributed Messaging

### Learning Objectives
1. Explain why batch processing alone is insufficient for modern data systems
2. Describe Kafka's architecture: brokers, topics, partitions, offsets, producer/consumer
3. Understand consumer groups and how Kafka achieves parallel consumption
4. Publish and consume messages using `kafka-console-producer` and `kafka-console-consumer`
5. Write a Python Kafka producer and consumer using `confluent-kafka`
6. Reason about partitioning strategies and their effect on ordering and parallelism

### Pre-Class Video
**"Apache Kafka in 6 Minutes"** — James Cutajar
🔗 https://www.youtube.com/watch?v=Ch5VhJzaoaI

**Alternative (deeper):** "Kafka 101" — Confluent
🔗 https://www.youtube.com/watch?v=j4bqyAMMb7o

### Materials
- 📊 Slides: `slides/SE446_W10A_kafka.pdf`
- 📓 Notebook: `notebooks/SE446_W10A_kafka.ipynb`

---

## Session 10B: Spark Structured Streaming

### Learning Objectives
1. Distinguish between batch processing, micro-batch streaming, and true event streaming
2. Explain the Structured Streaming model: "infinite table that keeps growing"
3. Read from Kafka topics in Spark using `readStream`
4. Apply transformations (filter, groupBy, window) on streaming DataFrames
5. Write results using `writeStream` with output modes: append, update, complete
6. Implement tumbling and sliding time windows for real-time aggregation

### Pre-Class Video
**"Spark Structured Streaming — Full Tutorial"** — Rock the JVM
🔗 https://www.youtube.com/watch?v=5Y1mIYnGaKE

**Alternative:** "Structured Streaming Programming Guide" — Databricks (official)
🔗 https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

### Materials
- 📊 Slides: `slides/SE446_W10B_structured_streaming.pdf`
- 📓 Notebook: `notebooks/SE446_W10B_structured_streaming.ipynb`

---

## Key Concepts

### Why Real-Time Processing?

Batch processing (Hive, Spark batch) analyses data that has **already arrived**. But many modern use cases require acting on data **as it happens**:

| Use Case | Latency Requirement | Batch OK? |
|----------|:-------------------:|:---------:|
| Monthly sales report | Hours | ✅ |
| Fraud detection during a transaction | Milliseconds | ❌ |
| Real-time dashboard of website traffic | Seconds | ❌ |
| IoT sensor anomaly detection | Sub-second | ❌ |
| Social media trending topics | Minutes | Borderline |

Kafka + Structured Streaming fill this gap by enabling **continuous** data ingestion and analysis.

### Kafka Architecture

```
                        ┌─────────────────────────────────────┐
                        │          KAFKA CLUSTER               │
                        │                                      │
 ┌──────────┐           │  Topic: "crimes"                     │           ┌──────────────┐
 │ Producer │──publish──▶│  ┌────────┬────────┬────────┐       │──consume──▶│ Consumer     │
 │ (app)    │           │  │ Part 0 │ Part 1 │ Part 2 │       │           │ Group A      │
 └──────────┘           │  │ off 0  │ off 0  │ off 0  │       │           └──────────────┘
                        │  │ off 1  │ off 1  │ off 1  │       │
 ┌──────────┐           │  │ off 2  │ off 2  │        │       │           ┌──────────────┐
 │ Producer │──publish──▶│  │ ...    │ ...    │        │       │──consume──▶│ Consumer     │
 │ (sensor) │           │  └────────┴────────┴────────┘       │           │ Group B      │
 └──────────┘           │                                      │           │ (Spark)      │
                        │  Broker 1   Broker 2   Broker 3      │           └──────────────┘
                        └─────────────────────────────────────┘
```

**Key terms:**
- **Broker**: A Kafka server that stores and serves messages
- **Topic**: A named feed of messages (like a database table)
- **Partition**: A topic is split into partitions for parallelism; each partition is an ordered, immutable log
- **Offset**: The position of a message within a partition (like a row number)
- **Producer**: Publishes messages to a topic
- **Consumer**: Reads messages from a topic
- **Consumer Group**: Multiple consumers sharing the work; each partition is read by exactly one consumer in the group

### Kafka vs Traditional Message Queues

| Feature | Kafka | RabbitMQ / ActiveMQ |
|---------|-------|---------------------|
| Storage | Persistent (configurable retention) | Transient (deleted after delivery) |
| Replay | Can re-read old messages by resetting offset | Cannot re-read |
| Throughput | Millions of messages/sec | Thousands/sec |
| Ordering | Per-partition guaranteed | Per-queue |
| Consumers | Multiple groups can read same data independently | Message consumed once |
| Use case | Event streaming, log aggregation | Task queues, RPC |

### Structured Streaming — The Infinite Table Model

Spark Structured Streaming treats a live stream as an **unbounded table** — new data rows keep arriving and appending to the bottom:

```
Time t1:  | crime_id | type    | district | hour |
          |----------|---------|----------|------|
          | 001      | THEFT   | 8        | 14   |
          | 002      | BATTERY | 11       | 22   |

Time t2:  | 001      | THEFT   | 8        | 14   |    ← existing rows
          | 002      | BATTERY | 11       | 22   |
          | 003      | ROBBERY | 3        | 02   |    ← NEW rows from Kafka
          | 004      | THEFT   | 8        | 03   |
```

### Output Modes

| Mode | Behaviour | Use Case |
|------|-----------|----------|
| **Append** | Only new rows are written | Alerting, logging |
| **Update** | Changed rows are re-written | Real-time dashboards |
| **Complete** | Entire result table is re-written | Aggregations (groupBy) |

### Time Windows

| Window Type | Description | Example |
|-------------|-------------|---------|
| **Tumbling** | Non-overlapping, fixed-size | "Count crimes every 5 minutes" |
| **Sliding** | Overlapping windows with slide interval | "Count crimes in 10-min windows, sliding every 2 min" |

```python
# Tumbling window: 5 minutes
df.groupBy(window("timestamp", "5 minutes"), "crime_type").count()

# Sliding window: 10-min window, 2-min slide
df.groupBy(window("timestamp", "10 minutes", "2 minutes"), "crime_type").count()
```

---

## Cluster Setup

Kafka runs alongside the existing HDFS/YARN/Spark stack on `134.209.172.50`:

| Service | Port | Purpose |
|---------|------|---------|
| Kafka Broker | 9092 | Message broker |
| ZooKeeper | 2181 | Kafka coordination (metadata) |
| Spark Master | 7077 | Spark standalone (if needed) |
| YARN ResourceManager | 8088 | YARN job management |
| HDFS NameNode | 9870 | HDFS Web UI |

### Key Kafka CLI Commands

```bash
# Create a topic
kafka-topics.sh --create --topic crimes --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic (partitions, replicas, ISR)
kafka-topics.sh --describe --topic crimes --bootstrap-server localhost:9092

# Produce messages (interactive)
kafka-console-producer.sh --topic crimes --bootstrap-server localhost:9092

# Consume messages (from beginning)
kafka-console-consumer.sh --topic crimes --bootstrap-server localhost:9092 \
    --from-beginning

# Consume with consumer group
kafka-console-consumer.sh --topic crimes --bootstrap-server localhost:9092 \
    --group my-group
```

---

## Connection to Previous & Next Weeks

```
Week 7: Spark Core        → In-memory processing (RDDs, DataFrames, SQL)
Week 8: Spark Cluster     → Optimization (caching, broadcast, partitions, Web UI)
Week 9: Spark MLlib       → Machine Learning at Scale
Week 10: THIS WEEK        → Kafka (10A) + Structured Streaming (10B)
Week 11: Hands-On Labs    → Kafka + Streaming labs
```

---

## Folder Structure

```
week10/
├── README.md                                    ← this file
├── slides/
│   ├── SE446_W10A_kafka.tex
│   ├── SE446_W10A_kafka.pdf
│   ├── SE446_W10B_structured_streaming.tex
│   └── SE446_W10B_structured_streaming.pdf
└── labs/                                        ← empty (labs in Week 11)
```

---

## Additional Resources

- 📖 [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- 📖 [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- 🎥 [Kafka Crash Course — Confluent](https://www.youtube.com/watch?v=R873BlNVUB4)
- 📖 [Kafka: The Definitive Guide](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/)
- 📖 [Learning Spark, 2nd Ed — Chapter 8: Structured Streaming](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
