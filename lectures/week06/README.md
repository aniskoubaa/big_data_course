# Week 6: YARN Resource Management + Advanced HiveQL

## Overview

Week 6 has two distinct but connected sessions. **Session 6A** fills a critical gap: YARN (Yet Another Resource Negotiator) — the cluster resource manager that has been running every MapReduce and Hive job since Week 2, but was never formally introduced. **Session 6B** goes deeper into HiveQL using a new dataset (NYC Yellow Taxi) to practice real analytical SQL: window functions, multi-table joins, ROLLUP/CUBE, and query optimization using `EXPLAIN`. This week also marks the **Midterm 1** period (Weeks 1–6 coverage).

---

## Session 6A: YARN — Cluster Resource Management

### Learning Objectives
1. Explain the limitations of Hadoop 1.0 (single JobTracker bottleneck)
2. Describe the YARN architecture: ResourceManager, NodeManager, ApplicationMaster, Containers
3. Trace the lifecycle of a YARN application (Hive query or MapReduce job)
4. Distinguish YARN scheduler types: FIFO, Fair Scheduler, Capacity Scheduler
5. Monitor running applications using the YARN Web UI and CLI
6. Interpret container allocation and resource limits in job logs

### Pre-Class Video
**"YARN Explained"** - IBM Technology (~12 min)  
🔗 https://www.youtube.com/watch?v=kQvkDjF2DpM

**Alternative**: "Apache YARN Architecture" - Simplilearn (~20 min)  
🔗 https://www.youtube.com/watch?v=FSzgQEV1Bqc

### Materials
- 📊 Slides: `slides/SE446_W06A_yarn_resource_management.pdf`
- 📓 Notebook: `notebooks/SE446_W06A_yarn_exercises.ipynb`

---

## Session 6B: Advanced HiveQL — NYC Taxi Analytics

### Learning Objectives
1. Load and query the NYC Yellow Taxi dataset in Hive
2. Write advanced `GROUP BY` queries with `HAVING`, `ROLLUP`, and `CUBE`
3. Apply window functions: `RANK()`, `ROW_NUMBER()`, `LAG()`, `LEAD()`, `NTILE()`
4. Perform multi-table JOINs (taxi trips + payment types + rate codes)
5. Use `EXPLAIN` to understand and optimize query execution plans
6. Compare query performance: TextFile vs ORC format

### Pre-Class Video
**"Advanced HiveQL Window Functions"** - Edureka (~25 min)  
🔗 https://www.youtube.com/watch?v=H7H9VKWiQFQ

### Materials
- 📊 Slides: `slides/SE446_W06B_advanced_hiveql.pdf`
- 📓 Notebook: `notebooks/SE446_W06B_hive_taxi_analytics.ipynb`
- 🗂️ Dataset: `data/nyc_taxi_2024_sample.csv` (10,000 rows) → HDFS `/data/nyc_taxi/`

---

## Key Concepts

### YARN Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          YARN ARCHITECTURE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                RESOURCE MANAGER (Master Node)                       │   │
│  │  ─────────────────────────────────────────────────────────────────  │   │
│  │  Scheduler: allocates CPU + RAM slots to applications               │   │
│  │  Applications Manager: accepts job submissions, monitors AM         │   │
│  └─────────────────────────────┬───────────────────────────────────────┘   │
│                                │                                            │
│               ┌────────────────┴─────────────────┐                         │
│               ▼                                  ▼                          │
│  ┌────────────────────────┐        ┌────────────────────────┐               │
│  │  WORKER NODE 1         │        │  WORKER NODE 2         │               │
│  │  ─────────────────     │        │  ─────────────────     │               │
│  │  NodeManager           │        │  NodeManager           │               │
│  │  (reports resources,   │        │  (reports resources,   │               │
│  │   launches containers) │        │   launches containers) │               │
│  │                        │        │                        │               │
│  │  ┌──────────────────┐  │        │  ┌──────────────────┐  │               │
│  │  │ ApplicationMaster│  │        │  │ Container (Task) │  │               │
│  │  │ (manages this    │  │        │  │ e.g. Map task    │  │               │
│  │  │  job's tasks)    │  │        │  │ or Hive operator │  │               │
│  │  └──────────────────┘  │        │  └──────────────────┘  │               │
│  │  ┌──────────────────┐  │        │  ┌──────────────────┐  │               │
│  │  │ Container (Task) │  │        │  │ Container (Task) │  │               │
│  │  └──────────────────┘  │        │  └──────────────────┘  │               │
│  └────────────────────────┘        └────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Hadoop 1.0 vs YARN (Hadoop 2.0+)

| Aspect | Hadoop 1.0 (MRv1) | YARN (Hadoop 2.0+) |
|---|---|---|
| Resource management | JobTracker (single point of failure) | ResourceManager (HA-capable) |
| Job tracking | TaskTracker per node | NodeManager per node |
| Framework support | MapReduce only | MapReduce, Spark, Hive, HBase, Flink |
| Scalability | ~4,000 nodes max | 10,000+ nodes |
| Fault tolerance | JobTracker failure = cluster down | RM failure handled by HA standby |

### YARN Application Lifecycle

```
1. Client submits application (beeline query / spark-submit / hadoop jar)
         │
         ▼
2. ResourceManager allocates container for ApplicationMaster (AM)
         │
         ▼
3. AM starts on a NodeManager, registers with ResourceManager
         │
         ▼
4. AM requests containers for tasks (Map tasks, Hive operators, Spark executors)
         │
         ▼
5. ResourceManager allocates containers on available NodeManagers
         │
         ▼
6. Tasks run inside containers; report progress to AM
         │
         ▼
7. On completion, AM reports to ResourceManager; containers released
```

### YARN Schedulers

| Scheduler | Strategy | Best For |
|---|---|---|
| FIFO | First-in-first-out; one job at a time | Development/testing |
| Fair Scheduler | Each job gets equal share of cluster | Multi-user clusters |
| Capacity Scheduler | Cluster divided into queues with guaranteed capacity | Enterprise (default in YARN) |

### YARN CLI Reference

```bash
# List running applications
yarn application -list

# Kill a running application
yarn application -kill application_1234567890_0001

# View application logs
yarn logs -applicationId application_1234567890_0001

# Check node status
yarn node -list

# Check queue status
yarn queue -status default

# Check cluster resources
yarn cluster --fail-on-no-results
```

### YARN Web UI

- **URL on our cluster:** `http://134.209.172.50:8088`
- Key pages:
  - **All Applications** — status, containers, memory/vCores used
  - **Nodes** — per-node resource availability
  - **Scheduler** — queue tree, allocated vs available capacity

---

### NYC Taxi Dataset — Schema

The NYC Yellow Taxi 2024 dataset (10,000-row sample) stored at `hdfs:///data/nyc_taxi/`:

| Column | Type | Description |
|---|---|---|
| `tpep_pickup_datetime` | STRING | Pickup timestamp |
| `tpep_dropoff_datetime` | STRING | Dropoff timestamp |
| `passenger_count` | INT | Number of passengers |
| `trip_distance` | DOUBLE | Trip distance in miles |
| `pickup_location_id` | INT | TLC taxi zone pickup ID |
| `dropoff_location_id` | INT | TLC taxi zone dropoff ID |
| `payment_type` | INT | 1=Credit card, 2=Cash, 3=No charge, 4=Dispute |
| `fare_amount` | DOUBLE | Base fare |
| `tip_amount` | DOUBLE | Tip amount |
| `total_amount` | DOUBLE | Total charged |
| `rate_code_id` | INT | 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated |

---

### Advanced HiveQL — Key Concepts

#### Window Functions
```sql
-- RANK: rank trips by fare within each payment type
SELECT payment_type, total_amount,
       RANK() OVER (PARTITION BY payment_type ORDER BY total_amount DESC) AS fare_rank
FROM taxi_trips;

-- ROW_NUMBER: assign a unique sequential number
SELECT tpep_pickup_datetime, total_amount,
       ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS row_num
FROM taxi_trips;

-- LAG / LEAD: compare with previous/next row
SELECT tpep_pickup_datetime, total_amount,
       LAG(total_amount, 1) OVER (ORDER BY tpep_pickup_datetime) AS prev_fare,
       total_amount - LAG(total_amount, 1) OVER (ORDER BY tpep_pickup_datetime) AS fare_diff
FROM taxi_trips;

-- NTILE: divide rows into N buckets
SELECT total_amount,
       NTILE(4) OVER (ORDER BY total_amount) AS quartile
FROM taxi_trips;
```

#### ROLLUP and CUBE
```sql
-- ROLLUP: subtotals at each level + grand total
SELECT payment_type, rate_code_id, COUNT(*), AVG(total_amount)
FROM taxi_trips
GROUP BY payment_type, rate_code_id WITH ROLLUP;

-- CUBE: all possible subtotal combinations
SELECT payment_type, rate_code_id, COUNT(*), AVG(total_amount)
FROM taxi_trips
GROUP BY payment_type, rate_code_id WITH CUBE;
```

#### Multi-Table JOIN
```sql
-- Join taxi trips with payment type lookup
CREATE TABLE payment_types (id INT, description STRING);

SELECT t.tpep_pickup_datetime, t.total_amount, p.description AS payment_desc
FROM taxi_trips t
JOIN payment_types p ON (t.payment_type = p.id)
WHERE t.total_amount > 20
ORDER BY t.total_amount DESC
LIMIT 20;

-- Left join to keep all trips (even with unknown payment type)
SELECT t.total_amount, p.description
FROM taxi_trips t
LEFT JOIN payment_types p ON (t.payment_type = p.id);
```

---

## Labs

### Lab: YARN Monitoring (Session 6A)

Students observe YARN during live query execution:

1. Open YARN Web UI at `http://134.209.172.50:8088`
2. In Beeline, run a slow Hive query on the crimes table (large `GROUP BY`)
3. While it runs, observe in YARN:
   - Application appears under "RUNNING"
   - Containers allocated on each worker node
   - Memory and vCores in use
4. Record from YARN:
   - Application ID
   - AM host
   - Number of containers
   - Peak memory used (MB)
5. After completion, retrieve logs: `yarn logs -applicationId <id>`

**Expected output:** filled lab report table with YARN metrics

---

### Lab: NYC Taxi Analytics in HiveQL (Session 6B)

```sql
-- 1. Create the external table
CREATE EXTERNAL TABLE nyc_taxi (
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type INT,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,
    rate_code_id INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/nyc_taxi/'
TBLPROPERTIES ("skip.header.line.count"="1");
```

**Exercises:**

| # | Query Task | SQL Concept |
|---|---|---|
| 1 | Count trips per payment type | `GROUP BY` |
| 2 | Average tip by number of passengers | `GROUP BY` + `AVG` |
| 3 | Top 5 most expensive trips | `ORDER BY` + `LIMIT` |
| 4 | Trips with tip > 20% of fare | `WHERE` with expression |
| 5 | Rank trips by total_amount within each rate_code | `RANK() OVER(PARTITION BY...)` |
| 6 | Computing running total of fares | `SUM() OVER(ORDER BY...)` |
| 7 | Trips subtotals by payment_type + rate_code | `GROUP BY ... WITH ROLLUP` |
| 8 | Join trips with payment type description | `JOIN` |
| 9 | Tip percentage quartile for credit card trips | `NTILE(4) OVER(...)` |
| 10| Show query execution plan for exercise 7 | `EXPLAIN` |

---

## Midterm 1 Overview

**Coverage:** Weeks 1–6 (HDFS, MapReduce, Hive, YARN)  
**Format:** Closed-book, in-class, 90 minutes  
**Suggested distribution:**

| Topic | Weight |
|---|---|
| HDFS Architecture & commands | 20% |
| MapReduce concepts & Python implementation | 25% |
| Hive architecture, table types, DDL | 25% |
| HiveQL queries (SELECT, GROUP BY, JOIN, Window) | 20% |
| YARN architecture & application lifecycle | 10% |

---

## Folder Structure

```
week06/
├── README.md                              ← this file
├── slides/
│   ├── _lecture_plan_w06.md              ← detailed slide-by-slide plan
│   ├── SE446_W06A_yarn_resource_management.tex
│   └── SE446_W06B_advanced_hiveql.tex
├── notebooks/
│   ├── SE446_W06A_yarn_exercises.ipynb   ← YARN CLI + monitoring notebook
│   └── SE446_W06B_hive_taxi_analytics.ipynb
├── labs/
│   └── 01_yarn_hive_lab/
│       └── lab_instructions.md
├── quizzes/
│   ├── quiz_plan.md
│   ├── quiz_week06_plan_female.md
│   ├── quiz_week06_plan_male.md
│   ├── SE446_quiz_W06_female.xml
│   └── SE446_quiz_W06_male.xml
└── data/
    └── nyc_taxi_2024_sample.csv          ← local sample (upload to HDFS before lab)
```

---

## Content Checklist

### Session 6A — YARN
- [ ] Slide deck: `SE446_W06A_yarn_resource_management.tex` (full LaTeX Beamer)
- [ ] Lecture plan: `slides/_lecture_plan_w06.md` — slide-by-slide plan
- [ ] Notebook: `SE446_W06A_yarn_exercises.ipynb` — YARN CLI commands + monitoring

### Session 6B — Advanced HiveQL
- [ ] Slide deck: `SE446_W06B_advanced_hiveql.tex` (full LaTeX Beamer)
- [ ] Notebook: `SE446_W06B_hive_taxi_analytics.ipynb`
- [ ] NYC Taxi dataset uploaded to HDFS: `hdfs dfs -put nyc_taxi_2024_sample.csv /data/nyc_taxi/`
- [ ] Lab instructions: `labs/01_yarn_hive_lab/lab_instructions.md`

### Quizzes
- [ ] `quiz_plan.md` — 10-question plan (5 MCQ + 5 numerical)
- [ ] `quiz_week06_plan_female.md` — 5Q female section
- [ ] `quiz_week06_plan_male.md` — 5Q male section
- [ ] `SE446_quiz_W06_female.xml` — Moodle XML
- [ ] `SE446_quiz_W06_male.xml` — Moodle XML

### Midterm 1
- [ ] Midterm 1 question bank (assessments/midterms/)
- [ ] Moodle exam configuration
- [ ] Study guide posted to students

---

## Additional Resources

- 📖 [Apache YARN Documentation](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
- 📖 [YARN Capacity Scheduler Guide](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html)
- 📖 [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- 🎥 [YARN Architecture Deep Dive — Hortonworks](https://www.youtube.com/watch?v=0y1e9JGCEiU)
- 📖 [HiveQL Window Functions Reference](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics)
