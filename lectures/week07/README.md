# Week 7-8: Apache Spark

## Overview

Weeks 7-8 introduce Apache Spark — the in-memory distributed computing framework that replaced MapReduce as the de facto engine for large-scale data processing. Students will learn the Spark programming model (RDDs, DataFrames, Spark SQL), execute jobs on the cluster, and apply Spark to real-world analytics tasks. **Milestone M3** (Spark Analytics project) is due at the end of Week 8.

---

## Session 7A: Spark Fundamentals — RDDs and the Spark Programming Model

### Learning Objectives
1. Explain why Spark outperforms MapReduce (in-memory DAG vs. disk-based MR)
2. Describe the Spark cluster architecture: Driver, Master, Workers, Executors
3. Understand RDDs (Resilient Distributed Datasets): creation, partitioning, lineage
4. Distinguish transformations (lazy) from actions (eager)
5. Write basic PySpark programs using `SparkContext`
6. Read data into RDDs from HDFS and local files

### Pre-Class Video
**"Apache Spark Explained"** - Simplilearn (~20 min)  
🔗 https://www.youtube.com/watch?v=QaoJNXW6SQo

**Alternative**: "What is Apache Spark?" - IBM Technology (~10 min)  
🔗 https://www.youtube.com/watch?v=4JP0XD-cNjQ

### Materials
- 📊 Slides: `slides/SE446_W07A_spark_fundamentals.pdf`
- 📓 Notebook: `notebooks/SE446_W07A_spark_rdd.ipynb`

---

## Session 7B: Spark DataFrames and Spark SQL

### Learning Objectives
1. Understand the DataFrame API and how it differs from RDDs
2. Create DataFrames from CSV, JSON, Parquet, and HDFS paths
3. Apply DataFrame transformations: `select`, `filter`, `groupBy`, `agg`, `join`
4. Use Spark SQL to query DataFrames with standard SQL syntax
5. Understand the Catalyst optimizer and Tungsten execution engine
6. Compare Spark SQL performance against raw MapReduce for the Chicago dataset

### Pre-Class Video
**"PySpark DataFrames Tutorial"** - Data with Zach (~25 min)  
🔗 https://www.youtube.com/watch?v=UZt_pEKFXxg

### Materials
- 📊 Slides: `slides/SE446_W07B_spark_dataframes.pdf`
- 📓 Notebook: `notebooks/SE446_W07B_spark_dataframes.ipynb`

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

### Materials
- 📊 Slides: `slides/SE446_W08A_spark_cluster.pdf`
- 📓 Notebook: `notebooks/SE446_W08A_spark_cluster.ipynb`
- 🔬 Lab: `labs/01_spark_rdd_lab/`

---

## Session 8B: Spark MLlib — Machine Learning at Scale

### Learning Objectives
1. Understand where MLlib fits in the Spark ecosystem
2. Use the ML Pipeline API: `Transformer`, `Estimator`, `Pipeline`
3. Prepare features with `VectorAssembler` and `StringIndexer`
4. Train and evaluate a classification model (Logistic Regression / Random Forest)
5. Apply MLlib to the Chicago crimes dataset: predict crime category from location + time
6. Evaluate model performance with `BinaryClassificationEvaluator` and confusion matrix

### Pre-Class Video
**"Spark MLlib Tutorial"** - Edureka (~30 min)  
🔗 https://www.youtube.com/watch?v=0HqM-jdmXaw

### Materials
- 📊 Slides: `slides/SE446_W08B_spark_mllib.pdf`
- 📓 Notebook: `notebooks/SE446_W08B_spark_mllib.ipynb`
- 🔬 Lab: `labs/02_spark_ml_lab/`

---

## Key Concepts

### Spark vs MapReduce

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MAPREDUCE vs SPARK                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MapReduce (Disk-Based)                                                     │
│  ───────────────────────                                                    │
│  Input (HDFS)                                                               │
│       │                                                                     │
│       ▼                                                                     │
│    [MAP] ──── write to disk ────> [SHUFFLE] ──── write to disk ──> [REDUCE] │
│                                                                             │
│  Every stage writes to HDFS. Iterative algorithms = N × disk I/O           │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  Spark (In-Memory DAG)                                                      │
│  ─────────────────────                                                      │
│  Input (HDFS / S3 / local)                                                  │
│       │                                                                     │
│       ▼                                                                     │
│    [RDD t1] ──▶ [RDD t2] ──▶ [RDD t3] ──▶ ACTION (result)                  │
│    (in RAM)     (in RAM)     (in RAM)                                       │
│                                                                             │
│  Intermediate results stay in memory. 10-100x faster for iterative work.   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### RDD Transformation vs Action

| Category      | Operation         | Description                             | Return Type |
|---------------|-------------------|-----------------------------------------|-------------|
| Transformation| `map(f)`          | Apply function to each element          | RDD         |
| Transformation| `filter(f)`       | Keep elements where f returns True      | RDD         |
| Transformation| `flatMap(f)`      | Map then flatten result                 | RDD         |
| Transformation| `groupByKey()`    | Group values by key (shuffle-heavy)     | RDD         |
| Transformation| `reduceByKey(f)`  | Reduce by key with combiner (efficient) | RDD         |
| Transformation| `sortByKey()`     | Sort RDD by key                         | RDD         |
| Action        | `collect()`       | Return all elements to driver           | List        |
| Action        | `count()`         | Count all elements                      | Int         |
| Action        | `take(n)`         | Return first n elements                 | List        |
| Action        | `saveAsTextFile`  | Write RDD to HDFS/local path            | None        |

> **Critical Rule:** Transformations are **lazy** — they build a DAG but execute nothing.  
> Calling an **action** triggers the actual computation.

---

### Spark Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       SPARK ON YARN CLUSTER                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  DRIVER (Master Node)                                              │    │
│  │  SparkContext → DAGScheduler → TaskScheduler → SchedulerBackend   │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                          │                                                  │
│                     YARN ResourceManager                                    │
│                          │                                                  │
│          ┌───────────────┴────────────────┐                                 │
│          ▼                                ▼                                 │
│  ┌──────────────────┐          ┌──────────────────┐                        │
│  │  Worker Node 1   │          │  Worker Node 2   │                        │
│  │  ─────────────   │          │  ─────────────   │                        │
│  │  NodeManager     │          │  NodeManager     │                        │
│  │  Executor        │          │  Executor        │                        │
│  │  ┌─────────────┐ │          │  ┌─────────────┐ │                        │
│  │  │  Task  Task │ │          │  │  Task  Task │ │                        │
│  │  │  [cache]    │ │          │  │  [cache]    │ │                        │
│  │  └─────────────┘ │          │  └─────────────┘ │                        │
│  └──────────────────┘          └──────────────────┘                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### DataFrame API vs RDD API

```python
# RDD approach (verbose, manual schema)
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()
data = rdd.filter(lambda line: line != header) \
           .map(lambda line: line.split(",")) \
           .filter(lambda f: len(f) > 5) \
           .map(lambda f: (f[5], 1)) \
           .reduceByKey(lambda a, b: a + b)

# DataFrame approach (concise, optimized by Catalyst)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()
df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)
df.groupBy("Primary Type").agg(count("*").alias("crime_count")) \
  .orderBy(col("crime_count").desc()).show(10)
```

---

## Labs

### Lab 1: Spark RDD Basics (`labs/01_spark_rdd_lab/`)
- Start PySpark shell on the cluster
- Load Chicago crimes CSV from HDFS into an RDD
- Count crimes per district using `map` + `reduceByKey`
- Count crimes per `Primary Type` using `groupByKey` vs `reduceByKey` — compare performance
- Save results to HDFS using `saveAsTextFile`
- Read the Spark Web UI job execution report

**Expected Output:** Text file on HDFS with crime counts, screenshot of Spark Web UI stages

---

### Lab 2: Spark ML Pipeline (`labs/02_spark_ml_lab/`)
- Load the crimes dataset into a Spark DataFrame
- Feature engineering: extract hour of day from timestamp, encode `District` as integer
- Assemble features with `VectorAssembler`
- Train a `RandomForestClassifier` to predict whether a crime resulted in arrest
- Evaluate with `MulticlassClassificationEvaluator` (accuracy, F1)
- Print feature importances

**Expected Output:** Model accuracy on test split, feature importance table

---

## Dataset

All labs use the Chicago Crimes dataset already on HDFS:

```
hdfs:///data/chicago_crimes.csv          ← full dataset (or sample)
```

Schema reminder:

| Column           | Type   | Description                          |
|------------------|--------|--------------------------------------|
| ID               | int    | Unique crime ID                      |
| Date             | string | Datetime of incident                 |
| Block            | string | Anonymized address block             |
| Primary Type     | string | Crime category (THEFT, BATTERY, ...) |
| District         | int    | Police district number               |
| Arrest           | boolean| Whether an arrest was made           |
| Domestic         | boolean| Whether domestic-related             |
| Year             | int    | Year of incident                     |

---

## Milestone M3 — Spark Analytics Project

**Due:** End of Week 8  
**Dataset:** Chicago Crimes (HDFS) — same dataset used in all prior labs

### Deliverables
1. **Spark RDD Analysis** — at least 3 RDD-based computations (e.g., top-10 crime types per year, district arrest rate trend)
2. **Spark DataFrame Analysis** — same 3 analyses rewritten using DataFrame / Spark SQL; compare code length and runtime
3. **ML Model** — train a binary classifier (Arrest = True/False) using at least 3 features; report accuracy, precision, recall
4. **Execution Report** — Spark Web UI screenshots showing stages, task distribution, executor metrics
5. **Written Report** — 2-page PDF: methodology, results, discussion of Spark vs MapReduce trade-offs

### Submission
- Jupyter Notebook: `M3_Spark_Analytics_<StudentID>.ipynb`
- Report PDF: `M3_Report_<StudentID>.pdf`
- Submit via Moodle by **23:59 Sunday, end of Week 8**

---

## Assessment

### Quiz (Week 8 — in-class, 25 min)
- 10 questions (5 MCQ + 5 numerical)
- Covers: Spark architecture, RDD transformations/actions, DataFrame API, Web UI metrics, MLlib pipeline
- See `quizzes/quiz_plan.md` for full question plan (male/female split)

### Grading Rubric for M3
| Component          | Points |
|--------------------|--------|
| RDD Analysis       | 25     |
| DataFrame Analysis | 25     |
| ML Model           | 30     |
| Execution Report   | 10     |
| Written Report     | 10     |
| **Total**          | **100**|

---

## Folder Structure

```
week07-08/
├── README.md                         ← this file (master plan)
├── slides/
│   ├── SE446_W07A_spark_fundamentals.tex
│   ├── SE446_W07B_spark_dataframes.tex
│   ├── SE446_W08A_spark_cluster.tex
│   └── SE446_W08B_spark_mllib.tex
├── notebooks/
│   ├── SE446_W07A_spark_rdd.ipynb
│   ├── SE446_W07B_spark_dataframes.ipynb
│   ├── SE446_W08A_spark_cluster.ipynb
│   └── SE446_W08B_spark_mllib.ipynb
├── labs/
│   ├── 01_spark_rdd_lab/
│   │   ├── lab_instructions.md
│   │   ├── mapper_crimes.py
│   │   └── spark_crimes_rdd.py
│   └── 02_spark_ml_lab/
│       ├── lab_instructions.md
│       └── spark_ml_pipeline.py
└── quizzes/
    ├── quiz_plan.md
    ├── quiz_week07_08_plan_female.md
    ├── quiz_week07_08_plan_male.md
    ├── SE446_quiz_W07_W08_female.xml
    ├── SE446_quiz_W07_W08_female_part2.xml
    ├── SE446_quiz_W07_W08_male.xml
    └── SE446_quiz_W07_W08_male_part2.xml
```

---

## Content Checklist

### Week 7
- [ ] Slide deck W07A: Spark Fundamentals (RDD, DAG, lazy evaluation)
- [ ] Slide deck W07B: DataFrames, Spark SQL, Catalyst optimizer
- [ ] Notebook W07A: RDD creation, transformations, actions, Chicago dataset
- [ ] Notebook W07B: DataFrame API, Spark SQL on Chicago dataset

### Week 8
- [ ] Slide deck W08A: Cluster deployment, `spark-submit`, Web UI, tuning
- [ ] Slide deck W08B: MLlib Pipeline API, feature engineering, model evaluation
- [ ] Notebook W08A: Spark cluster job, caching experiment, Web UI walkthrough
- [ ] Notebook W08B: MLlib arrest prediction pipeline
- [ ] Lab 01: Spark RDD lab instructions + starter scripts
- [ ] Lab 02: Spark ML pipeline lab instructions + starter scripts

### Quizzes
- [ ] `quiz_plan.md` — master 10-question plan
- [ ] `quiz_week07_08_plan_female.md` — 5Q female section
- [ ] `quiz_week07_08_plan_male.md` — 5Q male section
- [ ] `SE446_quiz_W07_W08_female.xml` — Moodle XML part 1
- [ ] `SE446_quiz_W07_W08_female_part2.xml` — Moodle XML part 2
- [ ] `SE446_quiz_W07_W08_male.xml` — Moodle XML part 1
- [ ] `SE446_quiz_W07_W08_male_part2.xml` — Moodle XML part 2

### Milestone
- [ ] M3 instructions published on Moodle
- [ ] Starter notebook template provided to students
- [ ] Grading rubric posted
