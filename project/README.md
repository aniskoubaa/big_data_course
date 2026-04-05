# SE446 Course Project: Chicago Crime Analytics

## Overview

The course project consists of **two milestones** that progressively build a complete data analytics pipeline on the Chicago Crimes dataset. Each milestone applies the technologies learned in class to answer real questions about crime patterns.

## Milestones

| Milestone | Topic | Technology | Due | Weight |
|-----------|-------|-----------|-----|--------|
| **M1** | Crime Analytics with MapReduce | Hadoop MapReduce (Python) | Week 6 | 10% |
| **M2** | Spark Analytics + ML Prediction | PySpark + Spark MLlib | Week 11 | 10% |

## Progression

```
M1: MapReduce Pipeline          M2: Spark Analytics + ML
(batch processing)              (in-memory + machine learning)

Count crimes by type    --->    DataFrame analysis (same questions, faster)
Count by location       --->    + Spark SQL queries
Trend over years        --->    + ML pipeline: predict arrests
Arrest rate             --->    + Model comparison & tuning
                                + Local + Cluster execution
```

M2 builds on M1: students revisit the same analytical questions using Spark (faster, cleaner code), then extend with ML to **predict** outcomes rather than just count them.

## Folder Structure

```
project/
├── README.md                          <-- this file
├── milestone_01_mapreduce/
│   ├── milestone_01_mapreduce.md      <-- M1 specification
│   └── _solution/                     <-- instructor only
└── milestone_02_spark_ml/
    ├── milestone_02_spark_ml.md       <-- M2 specification
    └── _solution/                     <-- instructor only
```

## Dataset

All milestones use the **Chicago Crimes dataset** (7M+ records, 2001-present):

- **Cluster**: `hdfs:///data/chicago_crimes.csv`
- **Local sample**: Generated in-code or downloaded from the course repo

## Group Policy

- **Group size**: 3-5 students (same groups for both milestones)
- **Git workflow**: One branch per task, pull requests, every member must commit
- **AI policy**: AI tools allowed for debugging and learning, not for generating entire solutions
