# SE 446: Big Data Engineering

## Course Information

| | |
|---|---|
| **University** | Alfaisal University |
| **Department** | Software Engineering |
| **Instructor** | Prof. Anis Koubaa |
| **Semester** | Spring 2026 |
| **Credits** | 3 (2 Lecture + 1 Lab) |
| **Prerequisites** | Database Systems, Python Programming |

## Course Description

This course teaches students to build and operate data systems at scale. Starting from distributed storage (HDFS) and batch processing (MapReduce, Hive), students progress through in-memory analytics (Apache Spark), machine learning at scale (Spark MLlib), and real-time streaming (Kafka + Structured Streaming). Each week pairs a lecture with a hands-on lab on a live Hadoop cluster.

## Learning Objectives

By the end of this course, students will be able to:

1. Design and operate a distributed storage system using HDFS
2. Write batch processing jobs using MapReduce and HiveQL
3. Manage cluster resources with YARN and monitor job execution
4. Build data analysis pipelines using Spark RDDs, DataFrames, and Spark SQL
5. Optimize Spark jobs through caching, partitioning, and broadcast joins
6. Train and evaluate distributed machine learning models with Spark MLlib
7. Build real-time data pipelines using Apache Kafka and Spark Structured Streaming
8. Reason about trade-offs between batch, micro-batch, and streaming architectures

## Technology Stack

| Layer | Technology | Weeks |
|-------|-----------|-------|
| Storage | HDFS (Hadoop 3.4) | 2 |
| Batch Processing | MapReduce (Python) | 3-4 |
| SQL Analytics | Apache Hive | 5-6 |
| Resource Management | YARN | 6 |
| In-Memory Analytics | Apache Spark (PySpark) | 7-8 |
| Machine Learning | Spark MLlib | 9 |
| Messaging | Apache Kafka | 10 |
| Stream Processing | Spark Structured Streaming | 10-11 |

## Weekly Schedule

| Week | Topic | Sessions | Milestone |
|------|-------|----------|-----------|
| 1 | Course Introduction + Tools Setup | 1A, 1B | -- |
| 2 | Introduction to Big Data + HDFS | 2A, 2B | -- |
| 3-4 | MapReduce + Data Formats | 3A, 3B, 4A, 4B | M1 |
| 5 | Apache Hive Fundamentals | 5A, 5B | -- |
| 6 | YARN + Advanced HiveQL | 6A, 6B | M2 + Midterm 1 |
| 7 | Apache Spark Core (RDDs + DataFrames) | 7A, 7B | -- |
| 8 | Spark on the Cluster + Performance Tuning | 8A (Midterm Mon) | M3 |
| 9 | Spark MLlib -- Machine Learning at Scale | 9A, 9B | -- |
| 10 | Apache Kafka + Structured Streaming | 10A, 10B | -- |
| 11 | Kafka + Streaming Hands-On Labs | 11A, 11B | M4 |
| 12 | Project Completion + Showcase | -- | M5 |
| 13 | Final Review | -- | -- |

## Prerequisite Chain

```
Week 2: HDFS ──────── Where data is stored (blocks, replication, NameNode/DataNode)
    │
    ▼
Week 3-4: MapReduce ── First distributed processing (map, shuffle, reduce)
    │
    ▼
Week 5-6: Hive + YARN ── SQL on Hadoop + cluster resource management
    │
    ▼
Week 7-8: Spark ────── In-memory processing (RDDs → DataFrames → Tuning)
    │
    ├──▶ Week 9: MLlib ──── Machine Learning at Scale
    │
    └──▶ Week 10-11: Kafka + Streaming ── Real-time data pipelines
```

## Repository Structure

```
big_data_course/
├── README.md                  ← this file
├── lectures/                  # Weekly slides (PDF), labs, notebooks
│   ├── week01/
│   ├── week02/
│   ├── ...
│   └── week11/
├── milestones/                # 5 progressive project milestones
├── data/                      # Sample datasets + schema documentation
├── resources/                 # Textbooks, docs, cheat sheets
└── assessments/               # Midterms, final, quizzes (structure only)
```

## Grading

| Component | Weight | Details |
|-----------|--------|---------|
| Midterm Exams (2) | 40% | Weeks 6 and 10 |
| Final Exam | 30% | Comprehensive |
| Quizzes (2) | 10% | In-class, Moodle |
| Project Milestones (5) | 20% | M1-M5, 4% each |

## Materials Strategy

| Type | Purpose | Location |
|------|---------|----------|
| **Slides** (PDF) | Lecture delivery | `lectures/weekXX/slides/` |
| **Notebooks** (`.ipynb`) | Hands-on tutorials + exercises | `lectures/weekXX/notebooks/` or `labs/` |
| **Labs** (`.md`) | Guided cluster exercises | `lectures/weekXX/labs/` |
| **Quizzes** | Assessment (ExamGPT + Moodle XML) | `lectures/weekXX/quizzes/` |

## Cluster Infrastructure

Students work on a live 3-node Hadoop cluster hosted on DigitalOcean:

- **Hadoop 3.4.1** (HDFS + YARN + MapReduce)
- **Apache Spark 3.5** (PySpark)
- **Apache Hive 4.0**
- **Apache Kafka 3.7**
- Web UIs accessible via HTTPS with authentication

## Contact

Prof. Anis Koubaa -- akoubaa@alfaisal.edu
GitHub: [github.com/aniskoubaa/big_data_course](https://github.com/aniskoubaa/big_data_course)
