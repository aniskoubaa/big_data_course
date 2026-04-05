# Week 9: Spark MLlib — Machine Learning at Scale

## Overview

Week 9 is dedicated to **Spark MLlib** — the distributed machine learning library built on top of Spark. **Session 9A** (Monday lecture) introduces the ML Pipeline API, feature engineering, model training, and evaluation. **Session 9B** (Wednesday hands-on) is a guided lab where students build an end-to-end ML pipeline to predict whether a crime results in an arrest using the Chicago Crimes dataset.

---

## Session 9A: Spark MLlib — Lecture

### Learning Objectives
1. Understand where MLlib fits in the Spark ecosystem
2. Use the ML Pipeline API: `Transformer`, `Estimator`, `Pipeline`
3. Prepare features with `VectorAssembler` and `StringIndexer`
4. Train and evaluate a classification model (Logistic Regression / Random Forest)
5. Apply MLlib to the Chicago crimes dataset: predict whether a crime results in arrest
6. Evaluate model performance with `BinaryClassificationEvaluator` and confusion matrix

### Pre-Class Video
**"Spark MLlib Tutorial"** - Edureka (~30 min)
🔗 https://www.youtube.com/watch?v=0HqM-jdmXaw

**Alternative**: "PySpark ML Tutorial" - Data with Zach (~25 min)
🔗 https://www.youtube.com/watch?v=SJz1WQcVswg

### Materials
- 📊 Slides: `slides/SE446_W09A_spark_mllib.pdf`
- 📓 Notebook: `notebooks/SE446_W09A_spark_mllib.ipynb`

---

## Session 9B: Spark MLlib — Hands-On Activity (Wednesday)

### Activity Objectives
1. Load and prepare the Chicago crimes dataset for ML
2. Perform feature engineering: `StringIndexer`, `VectorAssembler`
3. Build a `Pipeline` with feature stages + classifier
4. Train a `RandomForestClassifier` with `numTrees=100, maxDepth=5`
5. Evaluate: AUC-ROC, accuracy, precision, recall, F1 score
6. Print and interpret feature importances
7. Compare `LogisticRegression` vs `RandomForestClassifier` — which performs better?

### Materials
- 🔬 Lab: `labs/01_spark_ml_lab/`

---

## Key Concepts

### MLlib Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SPARK MLlib PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  RAW DATAFRAME                                                              │
│  ─────────────                                                              │
│  | District | Primary Type | Hour | Domestic | Arrest |                     │
│  |----------|-------------|------|----------|--------|                      │
│  | 8        | THEFT       | 14   | false    | false  |                      │
│  | 11       | BATTERY     | 22   | true     | true   |                      │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  STAGE 1: Feature Engineering (Transformers)                       │   │
│  │  StringIndexer: Primary Type → crime_type_index (0.0, 1.0, 2.0…)  │   │
│  │  StringIndexer: Domestic → domestic_index (0.0, 1.0)               │   │
│  │  VectorAssembler: [District, crime_type_index, Hour, domestic_idx] │   │
│  │                    → features (DenseVector)                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  STAGE 2: Train/Test Split                                         │   │
│  │  train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  STAGE 3: Model Training (Estimator)                               │   │
│  │  LogisticRegression(featuresCol="features", labelCol="Arrest")     │   │
│  │      OR                                                            │   │
│  │  RandomForestClassifier(numTrees=100, maxDepth=5)                  │   │
│  │                                                                     │   │
│  │  model = pipeline.fit(train_df)                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  STAGE 4: Prediction & Evaluation                                  │   │
│  │  predictions = model.transform(test_df)                            │   │
│  │  BinaryClassificationEvaluator → AUC-ROC                          │   │
│  │  MulticlassClassificationEvaluator → Accuracy, F1, Precision      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### MLlib Key Components

| Component | Type | Role | Example |
|---|---|---|---|
| `StringIndexer` | Transformer | Converts categorical column to numeric index | "THEFT" → 0.0, "BATTERY" → 1.0 |
| `OneHotEncoder` | Transformer | Converts index to one-hot vector | 0.0 → (3,[0],[1.0]) |
| `VectorAssembler` | Transformer | Combines multiple columns into a single feature vector | [8, 0.0, 14] → DenseVector |
| `StandardScaler` | Estimator | Normalizes features to zero mean, unit variance | z = (x - μ) / σ |
| `LogisticRegression` | Estimator | Binary/multi-class classification | Predict Arrest = True/False |
| `RandomForestClassifier` | Estimator | Ensemble of decision trees | Predict Arrest = True/False |
| `Pipeline` | Meta-Estimator | Chains multiple stages into a single workflow | Indexer → Assembler → Classifier |

### MLlib Code — End-to-End Example

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, \
    MulticlassClassificationEvaluator

# 1. Load data
spark = SparkSession.builder.appName("CrimeMLlib").getOrCreate()
df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)

# 2. Feature Engineering
from pyspark.sql.functions import hour, to_timestamp, col
df = df.withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
df = df.select("District", "Primary Type", "Hour", "Domestic", "Arrest").dropna()
df = df.withColumn("label", col("Arrest").cast("integer"))

# 3. Build Pipeline
crime_indexer = StringIndexer(inputCol="Primary Type", outputCol="crime_index",
                               handleInvalid="skip")
domestic_indexer = StringIndexer(inputCol="Domestic", outputCol="domestic_index",
                                  handleInvalid="skip")
assembler = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index"],
    outputCol="features"
)
rf = RandomForestClassifier(
    featuresCol="features", labelCol="label",
    numTrees=100, maxDepth=5, seed=42
)
pipeline = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, rf])

# 4. Train/Test Split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# 5. Train
model = pipeline.fit(train_df)

# 6. Predict
predictions = model.transform(test_df)

# 7. Evaluate
binary_eval = BinaryClassificationEvaluator(labelCol="label")
auc = binary_eval.evaluate(predictions)
print(f"AUC-ROC: {auc:.4f}")

mc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
print(f"Accuracy: {accuracy:.4f}")
print(f"F1 Score: {f1:.4f}")

# 8. Feature Importances
rf_model = model.stages[-1]
importances = rf_model.featureImportances
feature_names = ["District", "crime_index", "Hour", "domestic_index"]
for name, imp in zip(feature_names, importances):
    print(f"  {name}: {imp:.4f}")
```

---

## Dataset

All activities use the Chicago Crimes dataset already on HDFS:

```
hdfs:///data/chicago_crimes.csv          ← full dataset
```

Schema reminder:

| Column           | Type    | Description                          |
|------------------|---------|--------------------------------------|
| ID               | int     | Unique crime ID                      |
| Date             | string  | Datetime of incident                 |
| Block            | string  | Anonymized address block             |
| Primary Type     | string  | Crime category (THEFT, BATTERY, ...) |
| District         | int     | Police district number               |
| Arrest           | boolean | Whether an arrest was made           |
| Domestic         | boolean | Whether domestic-related             |
| Year             | int     | Year of incident                     |

---

## Connection to Previous & Next Weeks

```
Week 7: Spark Core    → In-memory processing (RDDs, DataFrames, SQL)
Week 8: Spark Cluster → Optimization (caching, broadcast, partitions, Web UI)
Week 9: THIS WEEK     → Machine Learning at Scale (MLlib)
Week 10: Kafka + Streaming → Real-time data processing
Week 11: Streaming Hands-On → Kafka + Spark Streaming labs
```

---

## Folder Structure

```
week09/
├── README.md                              ← this file
├── slides/
│   └── SE446_W09A_spark_mllib.tex
├── labs/
│   └── 01_spark_ml_lab/
│       └── lab_instructions.md
```

---

## Additional Resources

- 📖 [PySpark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- 📖 [ML Pipelines Documentation](https://spark.apache.org/docs/latest/ml-pipeline.html)
- 🎥 [Spark MLlib in Practice — Databricks](https://www.youtube.com/watch?v=9N-eLaJLiCI)
- 📖 [Learning Spark, 2nd Ed — Chapter 10: MLlib](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
