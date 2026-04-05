# Lab 09-0: Spark MLlib Local — Your First ML Pipeline

**Course:** SE446 — Big Data Systems
**Week:** 09 | Session 9B (Wednesday) | **Estimated Duration:** 45–60 minutes
**Environment:** Your laptop — no cluster, no HDFS needed

---

## Purpose

This is an **in-class, hands-on lab** designed to get you comfortable with the Spark MLlib Pipeline API on your own machine. You will build a complete ML pipeline — from raw data to trained model to evaluation — using PySpark in **local mode**.

The cluster lab (`01_spark_ml_lab/`) covers the same concepts on the full Chicago Crimes dataset on HDFS. This local lab is your warm-up: smaller data, same framework, same concepts.

By the end of this lab you will be able to:
- Start PySpark in local mode on your laptop
- Build a pipeline with `StringIndexer`, `VectorAssembler`, and a classifier
- Train and evaluate a Random Forest and Logistic Regression model
- Interpret a confusion matrix and feature importances
- Understand exactly what happens at each pipeline stage

---

## Prerequisites

- Python 3.8+ installed
- PySpark installed (`pip install pyspark`)
- No cluster access needed — everything runs locally

### Quick Install Check

```bash
# Verify PySpark is installed
python -c "import pyspark; print(pyspark.__version__)"

# If not installed:
pip install pyspark
```

> **Note:** PySpark includes a bundled Spark distribution. You do NOT need to install Spark or Hadoop separately for local mode.

---

## Setup

### Option A: Jupyter Notebook (Recommended)

```bash
pip install jupyter
jupyter notebook
```

Create a new Python 3 notebook and paste each code block as a cell.

### Option B: PySpark Shell

```bash
pyspark --master local[*]
```

This starts Spark using all available CPU cores on your machine.

---

## Part 1: Create a SparkSession and Load Data

In local mode, Spark runs entirely on your machine — no YARN, no HDFS. The data is a local CSV file that we will generate in the next step.

### Step 1: Start Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MLlib Local Lab") \
    .master("local[*]") \
    .getOrCreate()

# Reduce log noise
spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
print("Spark is running in LOCAL mode — no cluster needed!")
```

### Step 2: Generate a Sample Dataset

Instead of downloading the full Chicago Crimes dataset (7 million rows), we will generate a realistic sample directly in PySpark. This simulates the same schema and patterns.

```python
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
import random

random.seed(42)

# Crime types with realistic arrest rates
crime_profiles = {
    "NARCOTICS":        0.85,   # high arrest rate — caught in the act
    "PROSTITUTION":     0.80,
    "WEAPONS VIOLATION":0.60,
    "BATTERY":          0.30,
    "ASSAULT":          0.25,
    "ROBBERY":          0.15,
    "THEFT":            0.10,   # low arrest rate — rarely caught
    "BURGLARY":         0.08,
    "MOTOR VEHICLE THEFT": 0.06,
    "CRIMINAL DAMAGE":  0.05,
}

districts = list(range(1, 26))  # 25 districts

def generate_row():
    crime_type = random.choice(list(crime_profiles.keys()))
    base_rate = crime_profiles[crime_type]
    district = random.choice(districts)
    hour = random.randint(0, 23)
    domestic = random.random() < 0.15  # 15% are domestic

    # Arrest probability influenced by crime type, hour, domestic
    arrest_prob = base_rate
    if domestic:
        arrest_prob += 0.20   # mandatory arrest policies
    if 2 <= hour <= 5:
        arrest_prob -= 0.10   # harder to catch at night
    arrest_prob = max(0.01, min(0.99, arrest_prob))
    arrest = random.random() < arrest_prob

    return Row(
        District=district,
        PrimaryType=crime_type,
        Hour=hour,
        Domestic=domestic,
        Arrest=arrest
    )

# Generate 10,000 rows — small enough for your laptop, big enough to learn
rows = [generate_row() for _ in range(10000)]
df = spark.createDataFrame(rows)

print(f"Generated {df.count()} rows")
df.printSchema()
df.show(5)
```

### Step 3: Explore the Data

```python
# Class distribution
print("=== Arrest Distribution ===")
df.groupBy("Arrest").count().show()

# Crime types and their arrest rates
from pyspark.sql.functions import col, avg, count

print("=== Arrest Rate by Crime Type ===")
df.groupBy("PrimaryType") \
    .agg(
        count("*").alias("total"),
        avg(col("Arrest").cast("integer")).alias("arrest_rate")
    ) \
    .orderBy(col("arrest_rate").desc()) \
    .show()
```

### Questions — Part 1

1. What is the overall arrest rate in your generated dataset? Is the data balanced or imbalanced?
2. Which crime type has the highest arrest rate? Does this match the rates we programmed? Why might the actual rates differ slightly from the programmed ones?
3. We used `local[*]` as the master. What does the `*` mean? What would `local[2]` do instead?

---

## Part 2: Feature Engineering

### Step 1: Prepare the Label Column

```python
# Cast boolean Arrest to integer (True=1, False=0)
df = df.withColumn("label", col("Arrest").cast("integer"))

# Cast boolean Domestic to string (StringIndexer needs strings)
df = df.withColumn("Domestic_str", col("Domestic").cast("string"))

df.select("PrimaryType", "District", "Hour", "Domestic_str", "label").show(5)
```

### Step 2: Define Transformers

```python
from pyspark.ml.feature import StringIndexer, VectorAssembler

# Encode crime type: "THEFT" -> 0.0, "BATTERY" -> 1.0, ...
crime_indexer = StringIndexer(
    inputCol="PrimaryType",
    outputCol="crime_index",
    handleInvalid="skip"
)

# Encode domestic flag: "false" -> 0.0, "true" -> 1.0
domestic_indexer = StringIndexer(
    inputCol="Domestic_str",
    outputCol="domestic_index",
    handleInvalid="skip"
)

# Combine all numeric features into a single vector
assembler = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index"],
    outputCol="features"
)
```

### Step 3: See What Each Transformer Does

This is the most important part of the lab. Let's watch data transform step by step.

```python
# Fit the crime indexer on the full dataset (for visualization only!)
fitted_crime = crime_indexer.fit(df)
print("Crime type → index mapping:")
for i, label in enumerate(fitted_crime.labels[:5]):
    print(f"  {label:25s} → {float(i)}")
print(f"  ... ({len(fitted_crime.labels)} total types)")

# Apply all transformers manually to see intermediate results
temp = fitted_crime.transform(df)
temp = domestic_indexer.fit(df).transform(temp)
temp = assembler.transform(temp)

print("\n=== One row through the pipeline ===")
temp.select("PrimaryType", "crime_index",
            "District", "Hour",
            "Domestic_str", "domestic_index",
            "features", "label").show(3, truncate=False)
```

### Step 4: Train/Test Split

```python
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Training: {train_df.count()} rows")
print(f"Testing:  {test_df.count()} rows")

# Check class balance in training set
train_df.groupBy("label").count().show()
```

### Questions — Part 2

1. Look at the `features` column output. What does the vector `[8.0, 0.0, 14.0, 1.0]` represent? Map each position back to its original column.
2. Why does `VectorAssembler` need to run AFTER `StringIndexer`? What error would you get if you swapped the order?
3. We called `crime_indexer.fit(df)` on the full dataset for visualization. In the real pipeline, this is dangerous. Why? (Hint: data leakage)

---

## Part 3: Train a Random Forest

### Step 1: Build and Train the Pipeline

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
import time

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,      # fewer trees for local speed
    maxDepth=5,
    seed=42
)

# Chain everything into a Pipeline
pipeline_rf = Pipeline(stages=[
    crime_indexer,
    domestic_indexer,
    assembler,
    rf
])

# Train
print("Training Random Forest...")
t = time.time()
model_rf = pipeline_rf.fit(train_df)
print(f"Done in {time.time() - t:.1f}s")
```

### Step 2: Predict and Evaluate

```python
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

predictions_rf = model_rf.transform(test_df)

# See the output columns
predictions_rf.select("label", "prediction", "probability").show(5, truncate=False)

# Metrics
binary_eval = BinaryClassificationEvaluator(labelCol="label")
mc_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction")

auc_rf = binary_eval.evaluate(predictions_rf)
acc_rf = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "accuracy"})
f1_rf  = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "f1"})

print(f"AUC-ROC:  {auc_rf:.4f}")
print(f"Accuracy: {acc_rf:.4f}")
print(f"F1 Score: {f1_rf:.4f}")
```

### Step 3: Confusion Matrix

```python
print("=== Confusion Matrix (Random Forest) ===")
print("label=0: No Arrest | label=1: Arrest")
predictions_rf.groupBy("label", "prediction") \
    .count().orderBy("label", "prediction").show()
```

Record the four values: TN = ___, FP = ___, FN = ___, TP = ___

### Step 4: Feature Importances

```python
rf_model = model_rf.stages[-1]
feature_names = ["District", "crime_index", "Hour", "domestic_index"]
importances = rf_model.featureImportances.toArray()

print("=== Feature Importances ===")
for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
    bar = "#" * int(imp * 40)
    print(f"  {name:<18} {imp:.4f}  {bar}")
```

### Questions — Part 3

1. Which feature is most important? Does this match the data generation logic we programmed in Part 1?
2. Look at the `probability` column. What does `[0.72, 0.28]` mean? At what threshold does the model predict class 1?
3. We used `numTrees=50` instead of 100. How would increasing to 200 affect: (a) accuracy, (b) training time, (c) risk of overfitting?

---

## Part 4: Train Logistic Regression and Compare

### Step 1: Train

```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01
)

pipeline_lr = Pipeline(stages=[
    crime_indexer, domestic_indexer, assembler, lr
])

print("Training Logistic Regression...")
t = time.time()
model_lr = pipeline_lr.fit(train_df)
print(f"Done in {time.time() - t:.1f}s")
```

### Step 2: Evaluate

```python
predictions_lr = model_lr.transform(test_df)

auc_lr = binary_eval.evaluate(predictions_lr)
acc_lr = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "accuracy"})
f1_lr  = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "f1"})

print(f"AUC-ROC:  {auc_lr:.4f}")
print(f"Accuracy: {acc_lr:.4f}")
print(f"F1 Score: {f1_lr:.4f}")

# Confusion matrix
print("\n=== Confusion Matrix (Logistic Regression) ===")
predictions_lr.groupBy("label", "prediction") \
    .count().orderBy("label", "prediction").show()

# Coefficients
lr_model = model_lr.stages[-1]
coefficients = lr_model.coefficients.toArray()
print("=== LR Coefficients ===")
for name, coef in zip(feature_names, coefficients):
    direction = "(+) more arrest" if coef > 0 else "(-) less arrest"
    print(f"  {name:<18} {coef:+.4f}  {direction}")
```

### Step 3: Side-by-Side Comparison

```python
print("=" * 55)
print(f"{'Metric':<20} {'Random Forest':>15} {'Logistic Reg':>15}")
print("=" * 55)
print(f"{'AUC-ROC':<20} {auc_rf:>15.4f} {auc_lr:>15.4f}")
print(f"{'Accuracy':<20} {acc_rf:>15.4f} {acc_lr:>15.4f}")
print(f"{'F1 Score':<20} {f1_rf:>15.4f} {f1_lr:>15.4f}")
print("=" * 55)
winner = "Random Forest" if auc_rf > auc_lr else "Logistic Regression"
print(f"Better model (by AUC): {winner}")
```

### Questions — Part 4

1. Which model performed better on AUC-ROC? On F1? Are the rankings the same?
2. Look at the LR coefficients. Which feature has the strongest effect on arrest probability? Translate the coefficient into a plain-English statement.
3. LR trained faster than RF. In what scenarios would you choose the faster model even if it is slightly less accurate?
4. Both models were trained on 10,000 rows locally. When you move to the cluster lab with millions of rows, which model will benefit more from the extra data? Why?

---

## Part 5: Save and Reload (Bonus)

```python
import os
import tempfile

# Save to a local directory (not HDFS — we're in local mode)
save_path = os.path.join(tempfile.gettempdir(), "arrest_model_rf")
model_rf.save(save_path)
print(f"Model saved to: {save_path}")

# Reload
from pyspark.ml import PipelineModel
loaded = PipelineModel.load(save_path)

# Verify it works
loaded_preds = loaded.transform(test_df.limit(5))
loaded_preds.select("PrimaryType", "label", "prediction", "probability").show(truncate=False)
print("Model reloaded and working!")
```

### Questions — Part 5

1. We saved to `/tmp/`. On the cluster, you would save to `hdfs:///models/...`. Why is HDFS better than local disk for saving models in a distributed environment?
2. The saved model includes the fitted `StringIndexer` mappings. Why is this important? What would break if you saved only the classifier without the indexers?

---

## Cleanup

```python
spark.stop()
print("SparkSession stopped. Lab complete!")
```

---

## What's Next

You have just built a complete MLlib pipeline on your laptop. The **cluster lab** (`01_spark_ml_lab/`) does the same thing on the full 7-million-row Chicago Crimes dataset on HDFS:

| | This Lab (Local) | Cluster Lab |
|---|---|---|
| Data | 10,000 generated rows | 7M+ real rows on HDFS |
| Environment | `local[*]` on your laptop | YARN cluster via SSH |
| Speed | Seconds | Minutes |
| Purpose | Learn the framework | Experience distributed scale |

The concepts, API, and code structure are **identical** — only the data source and Spark master change.

---

## Deliverables

Submit your completed notebook (`.ipynb`) or script (`.py`) containing:

1. All code cells executed with output visible
2. Written answers to all questions (Parts 1–5)
3. The comparison table from Part 4, Step 3
4. Your confusion matrices for both models

---

## Grading

| Component | Points |
|-----------|:------:|
| Part 1: Setup + data exploration + 3 questions | 15 |
| Part 2: Feature engineering + transformer inspection + 3 questions | 20 |
| Part 3: Random Forest — train, evaluate, importances + 3 questions | 25 |
| Part 4: Logistic Regression — train, compare + 4 questions | 25 |
| Part 5: Save/reload + 2 questions | 15 |
| **Total** | **100** |
