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

## Submission

Each milestone has its own GitHub repository:

| Milestone | Repository | Submission |
|-----------|-----------|------------|
| M1 | `se446-project-group-X` | Submitted separately (already done) |
| M2 | `se446-m2-group-X` | Via **AssessX Group Project** |

## Group Policy

- **Group size**: 2-5 students
- **Same group** for both milestones
- **Git workflow**: One branch per task, pull requests, every member must commit

## Grading: Individual Grades Based on GitHub Activity

The project grade is **not a flat group grade**. Each member receives an individual grade adjusted by their GitHub contribution:

| Contribution Level | Grade |
|-------------------|-------|
| Strong contributor (meaningful commits, PRs, code authorship) | 100% of group score |
| Moderate contributor | 70-90% of group score |
| Minimal contributor | 30-60% of group score |
| No commits / ghost member | **0%** |

GitHub activity is analyzed automatically via **AssessX**. If your username does not appear in the commit history for substantive code, your individual grade is zero -- regardless of the group's score.

## Dataset

All milestones use the **Chicago Crimes dataset** (7M+ records, 2001-present):

- **Cluster**: `hdfs:///data/chicago_crimes.csv`
- **Local**: Generated in-code (10,000 rows) or downloaded sample

## AI Usage Policy

- **Allowed**: Debugging, conceptual explanations, generating comments
- **Prohibited**: Generating entire solutions from scratch
- **Penalty**: If you cannot explain your code during the in-class check, you receive a **Zero**

## Folder Structure (Instructor)

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
