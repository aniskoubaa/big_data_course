# Week 4: Apache Hive

## Overview

This week introduces Apache Hive — the SQL-on-Hadoop system that allows data analysts and engineers to query massive datasets stored in HDFS using a familiar SQL-like language (HiveQL). Students will learn Hive architecture, data model, and how to write analytical queries without writing low-level MapReduce code.

---

## Session 4A: Introduction to Apache Hive

### Learning Objectives
1. Explain how Hive provides a SQL interface to HDFS
2. Understand schema-on-read vs. schema-on-write
3. Describe Hive architecture (Driver, Metastore, Execution Engine)
4. Distinguish managed vs. external tables
5. Explain partitioning and bucketing strategies
6. Compare file formats: TextFile, ORC, Parquet

### Pre-Class Video
**"Apache Hive Tutorial"** - Simplilearn (~25 min)  
🔗 https://www.youtube.com/watch?v=tKNGB5IZPFE

**Alternative**: "Hive Explained in 10 Minutes" - ByteByteGo  
🔗 https://www.youtube.com/watch?v=Hs1S1JhS7lY

### Materials
- 📊 Slides: `slides/SE446_W04A_hive_fundamentals.pdf`

---

## Session 4B: HiveQL Queries in Practice

### Learning Objectives
1. Create databases and tables in Hive (DDL)
2. Load data from local filesystem and HDFS into Hive tables
3. Write analytical queries using SELECT, GROUP BY, HAVING, ORDER BY
4. Perform JOINs across tables
5. Use built-in string, date, numeric, and window functions
6. Optimize queries using partitioning, ORC format, and EXPLAIN

### Pre-Class Video
**"HiveQL Tutorial for Beginners"** - Edureka (~30 min)  
🔗 https://www.youtube.com/watch?v=2H_yVFGEp3A

### Materials
- 📊 Slides: `slides/SE446_W04B_hiveql_queries.pdf`

---

## Key Concepts

### Hive in the Hadoop Ecosystem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HIVE ARCHITECTURE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  USER (SQL)                                                                 │
│  ──────────                                                                 │
│  SELECT primary_type, COUNT(*) FROM crimes GROUP BY primary_type;           │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │  HiveServer2 (Thrift Server)                                     │      │
│  │  Accepts Beeline, JDBC, ODBC connections                         │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │  DRIVER                                                          │      │
│  │  ─────────────────────────────────────────────────────────────   │      │
│  │  1. Parser:    SQL → Abstract Syntax Tree (AST)                 │      │
│  │  2. Compiler:  AST → Logical Plan                                │      │
│  │  3. Optimizer: Predicate pushdown, partition pruning             │      │
│  │  4. Executor:  Physical plan → MapReduce / Tez / Spark jobs     │      │
│  └──────────────────┬─────────────────────┬─────────────────────────┘      │
│                     │                     │                                  │
│           ┌─────────▼─────────┐ ┌────────▼──────────┐                      │
│           │     METASTORE     │ │  EXECUTION ENGINE  │                      │
│           │  (MySQL/Derby)    │ │  (Tez/MR/Spark)    │                      │
│           │  - Table schemas  │ │  - Runs on YARN    │                      │
│           │  - Partition info │ │  - Reads from HDFS │                      │
│           │  - File locations │ │                    │                      │
│           └───────────────────┘ └────────────────────┘                      │
│                     │                     │                                  │
│                     ▼                     ▼                                  │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                            HDFS                                  │      │
│  │  /warehouse/crimes/year=2023/part-00000.orc                      │      │
│  │  /warehouse/crimes/year=2024/part-00000.orc                      │      │
│  └──────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Managed vs. External Tables

| Feature | Managed Table | External Table |
|---------|--------------|----------------|
| Data ownership | Hive owns data | Hive only owns metadata |
| DROP TABLE | Deletes data + metadata | Deletes metadata only |
| Data location | Hive warehouse dir | Custom HDFS location |
| Best for | Intermediate/derived data | Raw/shared data |

### Partitioning vs. Bucketing

| Feature | Partitioning | Bucketing |
|---------|-------------|-----------|
| Physical layout | Subdirectories | Files within directory |
| Based on | Column value | Hash of column |
| Cardinality | Low (year, country) | High (user_id) |
| Benefit | Partition pruning | Efficient joins & sampling |

### File Format Comparison

| Format | Type | Compressed | Best For |
|--------|------|-----------|----------|
| TextFile | Row | No | Simple CSV input |
| ORC | Columnar | Yes | Hive-optimized analytics |
| Parquet | Columnar | Yes | Cross-platform (Spark) |
| Avro | Row | Yes | Schema evolution |

---

## HiveQL Quick Reference

### DDL Commands
```sql
-- Create database
CREATE DATABASE IF NOT EXISTS my_db;
USE my_db;

-- Create external table from CSV
CREATE EXTERNAL TABLE crimes (
    case_number STRING, primary_type STRING, arrest BOOLEAN, district INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/crimes/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create partitioned ORC table
CREATE TABLE crimes_orc (...)
PARTITIONED BY (year INT)
STORED AS ORC;

-- Inspect table
DESCRIBE FORMATTED crimes;
SHOW PARTITIONS crimes_orc;
```

### Loading Data
```sql
-- From local filesystem
LOAD DATA LOCAL INPATH '/path/to/file.csv' INTO TABLE crimes;

-- From HDFS (moves the file!)
LOAD DATA INPATH '/staging/file.csv' INTO TABLE crimes;

-- Insert from another table (ETL)
INSERT OVERWRITE TABLE crimes_orc PARTITION (year)
SELECT *, YEAR(date_str) AS year FROM crimes;

-- CTAS: Create + populate in one step
CREATE TABLE theft_crimes STORED AS ORC AS
SELECT * FROM crimes WHERE primary_type = 'THEFT';
```

### Analytical Queries
```sql
-- Top crime types
SELECT primary_type, COUNT(*) AS cnt
FROM crimes GROUP BY primary_type
ORDER BY cnt DESC LIMIT 10;

-- Arrest rate by district
SELECT district, COUNT(*) AS total,
       ROUND(SUM(CASE WHEN arrest THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS pct
FROM crimes GROUP BY district;

-- Window function: rank within district
SELECT district, primary_type, crime_count,
       RANK() OVER (PARTITION BY district ORDER BY crime_count DESC) AS rnk
FROM crime_summary;
```

---

## Datasets Used This Week

| Dataset | Source | Description |
|---------|--------|-------------|
| `chicago_crimes_sample.csv` | `data/` folder | 30 crime incidents |
| `nyc_taxi_sample.csv` | `data/` folder | 50 taxi trips |
| `nyc_weather_sample.csv` | `data/` folder | 40 daily weather records |

---

## Lab Deliverables

1. **Database setup**: Create team database on the Hive cluster
2. **Table creation**: Load Chicago crimes into both TextFile and ORC tables
3. **Analytical queries**: Write 5+ HiveQL queries with GROUP BY, JOINs, and window functions
4. **Performance comparison**: Compare query times between TextFile and ORC
5. **Export results**: Save top-10 crime types to HDFS as CSV
6. **Commit**: Push all `.hql` scripts to your team GitHub repo

---

## Additional Resources

- 📖 [Apache Hive Documentation](https://hive.apache.org/)
- 📖 [HiveQL Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
- 🎥 [Hive Architecture Deep Dive - Cloudera](https://www.youtube.com/watch?v=8jMV9F0xUKk)
- 📖 [ORC File Format Specification](https://orc.apache.org/specification/)
