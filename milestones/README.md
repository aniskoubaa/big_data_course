# Milestones

Five progressive project milestones that build on each other throughout the semester. Each milestone applies the technology learned that week to the Chicago Crimes dataset.

## Overview

| Milestone | Topic | Technology | Week Due | Weight |
|-----------|-------|-----------|----------|--------|
| M1 | Data Loading + Exploration | HDFS + Python | Week 4 | 4% |
| M2 | Batch Processing | MapReduce + Hive | Week 6 | 4% |
| M3 | Spark Analytics | Spark RDD + DataFrame + MLlib | Week 8 | 4% |
| M4 | Streaming Pipeline | Kafka + Structured Streaming | Week 11 | 4% |
| M5 | Final Integration | End-to-end pipeline | Week 12 | 4% |

## Grading Per Milestone (4%)

| Component | Weight | Description |
|-----------|--------|-------------|
| GitHub AI Commit Analysis | 2% | Quality, frequency, and meaningfulness of commits |
| ExamGPT In-Class Quiz | 2% | Short quiz on the milestone's concepts |

## Progression

Each milestone builds on the previous one, forming a complete data pipeline:

```
M1: Load Data ──▶ M2: Process (MR/Hive) ──▶ M3: Analyze (Spark + ML)
                                                      │
                         M5: Integrate All ◀── M4: Stream (Kafka)
```

## Timeline

```
Week 1-2  │ Foundations (HDFS)
Week 3    │ ────────────────── M1 Start
Week 4    │ ────────────────── M1 Due
Week 5    │ ────────────────── M2 Start
Week 6    │ ────────────────── M2 Due + MIDTERM 1
Week 7    │ ────────────────── M3 Start
Week 8    │ ────────────────── M3 Due
Week 9-10 │ ────────────────── M4 Start + MIDTERM 2
Week 11   │ ────────────────── M4 Due
Week 12   │ ────────────────── M5 Due + Showcase
Week 13   │ Final Review
```

## Dataset

All milestones use the **Chicago Crimes dataset** (7M+ records). See `data/README.md` for schema details and download links.

## Submission

- **Jupyter Notebook**: `MX_Topic_<StudentID>.ipynb`
- **Report PDF**: `MX_Report_<StudentID>.pdf`
- **Submit via**: Moodle by 23:59 on the due date
- **GitHub**: Code committed to team repository with meaningful commit messages

## Folder Structure

```
milestones/
├── README.md              ← this file
├── M1_data_loading/
│   ├── instructions.md
│   ├── rubric.md
│   └── starter_notebook.ipynb
├── M2_batch_processing/
│   ├── instructions.md
│   └── rubric.md
├── M3_spark_analytics/
│   ├── instructions.md
│   └── rubric.md
├── M4_streaming/
│   ├── instructions.md
│   └── rubric.md
└── M5_integration/
    ├── instructions.md
    └── rubric.md
```
