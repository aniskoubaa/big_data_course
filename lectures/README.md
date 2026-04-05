# Lectures

Weekly lecture materials organized by week. Each week contains slides, labs, and optional notebooks.

## Weekly Structure

Every week follows a consistent pattern:

```
lectures/weekXX/
├── README.md          # Overview, learning objectives, session plan
├── slides/            # LaTeX source (.tex) + compiled PDF
│   ├── current/       # Latest version of slides
│   ├── notes/         # Instructor notes (walkthrough + koubaa style)
│   └── v1/            # Previous versions (if any)
├── labs/              # Hands-on lab instructions (.md) + notebooks (.ipynb)
└── quizzes/           # ExamGPT (.json) + Moodle (.xml)
```

## Weeks at a Glance

| Week | Topic | Sessions | Key Tools |
|------|-------|----------|-----------|
| 01 | Course Introduction + Tools | 1A, 1B | Colab, GitHub, Databricks |
| 02 | Big Data + HDFS | 2A, 2B | Hadoop, HDFS CLI |
| 03-04 | MapReduce + Hive | 3A, 3B, 4A, 4B | Python MapReduce, HiveQL |
| 05 | Hive Fundamentals | 5A, 5B | HiveQL, Beeline |
| 06 | YARN + Advanced HiveQL | 6A, 6B | YARN CLI, Window Functions |
| 07 | Spark Core (RDDs + DataFrames) | 7A, 7B | PySpark, Spark SQL |
| 08 | Spark Cluster + Tuning | 8A | spark-submit, Web UI |
| 09 | Spark MLlib | 9A, 9B | MLlib Pipeline, CrossValidator |
| 10 | Kafka + Structured Streaming | 10A, 10B | Kafka CLI, readStream |
| 11 | Streaming Hands-On Labs | 11A, 11B | Kafka + Spark labs |

## Naming Conventions

| Material | Pattern | Example |
|----------|---------|---------|
| Slides | `SE446_W0XY_topic.tex/.pdf` | `SE446_W09A_spark_mllib.pdf` |
| Notebooks | `SE446_W0XY_topic.ipynb` | `SE446_W09B_spark_mllib_lab.ipynb` |
| Instructor Notes | `SE446_W0XY_topic_notes_style.tex/.pdf` | `SE446_W09A_spark_mllib_notes_walkthrough.pdf` |
| Quizzes (ExamGPT) | `SE446_W0X_quiz.json` | `SE446_W08_quiz.json` |
| Quizzes (Moodle) | `SE446_W0X_quiz.xml` | `SE446_W08_quiz.xml` |

## Notebook Standards

Notebooks are educational tutorials, not assessments.

**Must include:**
1. Learning objectives
2. Concept explanations with examples
3. Step-by-step code tutorials
4. Coding exercises (fill-in-the-blank or build a function)
5. Test cells to verify solutions

**Must NOT include:** Quiz or exam questions (use ExamGPT/Moodle).

**Placeholder convention** for student exercises:
```python
def calculate_average(df):
    result = None  # TODO: Replace None with df['age'].mean()
    return result
```

## Instructor Notes

Two narration styles are generated for each lecture:

| Style | File suffix | Purpose |
|-------|------------|---------|
| **Walkthrough** | `_notes_walkthrough` | Live classroom delivery -- conversational, points at visuals |
| **Koubaa** | `_notes_koubaa` | Recorded video narration -- structured, scenario-driven |

Notes are compiled as 2-on-1 PDFs (slide on top, notes below).
