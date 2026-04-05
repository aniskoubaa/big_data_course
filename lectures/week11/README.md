# Week 11: Kafka & Spark Streaming — Hands-On Labs

## Overview

Week 11 is dedicated to **hands-on activities** for the real-time processing topics covered in Week 10. **Session 11A** (Monday) covers Kafka fundamentals lab — creating topics, producing and consuming messages. **Session 11B** (Wednesday) covers Spark Structured Streaming lab — building an end-to-end pipeline from Kafka to Spark to real-time analysis.

---

## Session 11A: Kafka Fundamentals Lab

### Lab Objectives
1. Create Kafka topics with multiple partitions
2. Produce and consume messages using CLI tools
3. Write a Python Kafka producer and consumer
4. Observe consumer group behavior and partition assignment
5. Experiment with message keys and partitioning strategies

### Materials
- 🔬 Lab: `labs/01_kafka_fundamentals_lab/`

---

## Session 11B: Spark Structured Streaming Lab

### Lab Objectives
1. Read from a Kafka topic using `readStream`
2. Apply transformations on streaming DataFrames (filter, groupBy)
3. Write results using different output modes (append, update, complete)
4. Implement tumbling and sliding time windows
5. Build an end-to-end pipeline: Kafka → Spark → real-time dashboard

### Materials
- 🔬 Lab: `labs/02_spark_streaming_lab/`

---

## Connection to Previous Weeks

```
Week 8: Spark Cluster     → Optimization (caching, broadcast, partitions)
Week 9: Spark MLlib       → Machine Learning at Scale
Week 10: Kafka + Streaming → Lectures on real-time data processing
Week 11: THIS WEEK        → Hands-on labs for Kafka + Streaming
```

---

## Folder Structure

```
week11/
├── README.md                              ← this file
├── labs/
│   ├── 01_kafka_fundamentals_lab/
│   │   └── lab_instructions.md
│   └── 02_spark_streaming_lab/
│       └── lab_instructions.md
```
