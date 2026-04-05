# Lab 09-1: Spark MLlib — Arrest Prediction Pipeline (Cluster)

**Course:** SE446 — Big Data Systems
**Week:** 09 | **Estimated Duration:** 90 minutes
**Environment:** Cluster — requires SSH access and HDFS

---

## Overview

In this lab you will build a **complete, end-to-end machine learning pipeline** using **Spark MLlib** to predict whether a crime in Chicago resulted in an arrest. The dataset has millions of rows — far too large to process comfortably on a single machine — which is exactly why we use Spark.

By the end of this lab you will be able to:
- Explain why traditional ML libraries (scikit-learn, pandas) are inadequate for large datasets.
- Construct a Spark MLlib `Pipeline` with feature transformers and a classifier.
- Evaluate a binary classifier correctly using AUC, accuracy, F1, and a confusion matrix.
- Interpret feature importances and explain model decisions in plain language.
- Compare two classifier families (tree-based vs. linear) and choose appropriately.

---

## Background Concepts

> Read this section carefully. The code will make much more sense once you understand the *why*.

### 1. Why Spark for Machine Learning?

Traditional ML libraries like scikit-learn load the entire dataset into RAM on a single machine. With millions of rows and many features, this either crashes or runs too slowly:

```
scikit-learn:  all data → one machine's RAM → one CPU core → slow / crashes
Spark MLlib:   data partitioned across cluster → parallel training tasks → fast
```

Spark MLlib is designed to run ML algorithms across a distributed cluster. The transformations and model fitting happen **in parallel across all partitions**, and the result is assembled automatically. You write code that looks almost identical to scikit-learn, but it runs on terabytes.

### 2. The ML Pipeline — Keeping Steps Ordered and Reproducible

A `Pipeline` chains together a sequence of `Transformer` and `Estimator` objects:

- **Transformer**: Takes a DataFrame and produces a new one (e.g., `StringIndexer`, `VectorAssembler`). It is stateless — you can apply it at any time.
- **Estimator**: Learns something from the training data and then produces a Transformer (e.g., `RandomForestClassifier.fit()` → `RandomForestModel`).

Why use a Pipeline instead of calling each step manually?

```
Without Pipeline:
  indexer.fit(train).transform(train) → assembler.transform(...) → rf.fit(...)
  indexer.fit(train).transform(test)  → assembler.transform(...) → rf.transform(...)
  ← easy to accidentally apply wrong transforms, use training stats on test data

With Pipeline:
  pipeline.fit(train)         ← fits all stages in order on training data
  pipeline.transform(test)    ← applies fitted transformers to test data safely
```

The Pipeline guarantees that the **same transformations learned on training data** are applied to the test data — preventing data leakage.

### 3. Feature Engineering — Turning Raw Data into Numbers

ML algorithms operate on **numerical vectors**. Our raw data contains strings (crime type, boolean flags, dates). We need to convert everything:

| Raw Column | Problem | Spark Transformer | Output |
|-----------|---------|-------------------|--------|
| `"Primary Type"` (e.g., "THEFT") | Categorical string | `StringIndexer` | Integer index (0, 1, 2, …) |
| `"Domestic"` (True/False) | Boolean string | `StringIndexer` | 0 or 1 |
| `"Date"` (timestamp) | Not numeric | `to_timestamp` + `hour()` | Integer 0–23 |
| All numeric columns | Separate columns | `VectorAssembler` | Single feature vector |

`VectorAssembler` is the crucial last step — it merges all selected columns into a single column called `features`, which is what every MLlib classifier expects.

### 4. Class Imbalance — Why Accuracy Can Lie

Imagine 80% of crimes did **not** result in an arrest. A model that predicts "no arrest" for every crime would achieve **80% accuracy** — without learning anything useful. This is the class imbalance problem.

Better metrics for imbalanced data:
- **AUC-ROC** (Area Under the ROC Curve): Measures how well the model separates the two classes across all decision thresholds. Perfect model = 1.0; random guessing = 0.5.
- **F1 Score**: Harmonic mean of Precision and Recall. Punishes models that ignore the minority class.
- **Confusion Matrix**: Shows exactly how many true positives, false positives, true negatives, and false negatives your model produced.

### 5. Random Forest vs. Logistic Regression

| Property | Random Forest | Logistic Regression |
|----------|--------------|---------------------|
| Type | Ensemble of decision trees | Linear classifier |
| Handles non-linear patterns? | Yes | No (without feature engineering) |
| Interpretable? | Partially (feature importances) | Yes (coefficients) |
| Trains fast? | Slower (many trees) | Faster (gradient descent) |
| Works well with mixed features? | Yes | Yes, but needs scaling |
| Baseline accuracy | Usually higher | Lower, but robust |

Neither is universally better. The right choice depends on the problem, data size, and interpretability requirements.

---

## Prerequisites

- SSH access to the cluster master node
- Dataset at `hdfs:///data/chicago_crimes.csv`
- PySpark shell working on YARN

---

## Setup

```bash
ssh hadoop@134.209.172.50

pyspark --master yarn --deploy-mode client \
    --num-executors 2 --executor-memory 1g --executor-cores 2 \
    --conf spark.sql.shuffle.partitions=4
```

Setting `spark.sql.shuffle.partitions=4` matches our cluster's 4 total cores, preventing the default 200 shuffle partitions from creating excessive overhead on a small cluster.

---

## Part 1: Data Exploration and Preparation

**Goal:** Understand the raw data, extract useful features, and define the target variable (label).

### Step 1: Load and Inspect

```python
from pyspark.sql.functions import col, hour, to_timestamp

df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)

print(f"Total rows: {df.count():,}")
print(f"Total columns: {len(df.columns)}")
df.printSchema()
df.show(5, truncate=False)
```

Look at the schema output. Notice that `inferSchema=True` detects column types automatically — but not always correctly. Examine which columns Spark inferred as `string` vs `integer`.

### Step 2: Feature Extraction

We derive the `Hour` feature from the `Date` timestamp. Time-of-day is a meaningful predictor — certain crimes and patterns of arrest differ by hour.

```python
# Extract hour of day from date string
df = df.withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))

# Select only the columns we need and drop rows with missing values
df = df.select("District", "Primary Type", "Hour", "Domestic", "Arrest", "Year").dropna()

# Create the binary label: 1 = arrested, 0 = not arrested
df = df.withColumn("label", col("Arrest").cast("integer"))

print(f"Rows after cleaning: {df.count():,}")
df.show(5)
```

### Step 3: Examine Class Balance

```python
print("=== Class Distribution ===")
label_counts = df.groupBy("label").count().orderBy("label")
label_counts.show()

total = df.count()
label_counts.withColumn("percentage", (col("count") / total * 100).cast("decimal(5,2)")).show()
```

Record: Arrest rate = \_\_\_\_\_% of all crimes.

### Step 4: Explore Key Features

```python
# What crime types appear most often?
print("=== Top 10 Crime Types ===")
df.groupBy("Primary Type").count().orderBy(col("count").desc()).show(10)

# Are certain hours more associated with arrests?
print("=== Arrests by Hour ===")
df.groupBy("Hour", "label").count().orderBy("Hour", "label").show(24)
```

### Questions — Part 1

1. What is the arrest rate in this dataset? Does this constitute class imbalance? At what ratio does class imbalance typically start affecting model performance?
2. Why did we extract the `Hour` from the `Date` column rather than passing the raw timestamp to the model? What problem would passing a raw timestamp string cause?
3. We called `.dropna()` on the selected columns. What is the risk of dropping rows with missing values? Suggest an alternative strategy for handling missing data in a production ML pipeline.
4. Look at the top crime types. Do you expect "THEFT" and "BATTERY" to have different arrest rates? Explain why this matters for the model.
5. *(Challenge)* What would happen if you included `"Arrest"` as an input feature (alongside it being the label)? What is this problem called in machine learning?

---

## Part 2: Feature Engineering with Transformers

**Goal:** Convert categorical strings into numerical indices and assemble all features into a single vector column.

### Step 1: Define Transformers

```python
from pyspark.ml.feature import StringIndexer, VectorAssembler

# StringIndexer assigns an integer to each unique string value
# The most frequent value gets index 0, the next gets 1, and so on
crime_indexer = StringIndexer(
    inputCol="Primary Type",
    outputCol="crime_index",
    handleInvalid="skip"   # skip rows with unseen crime types at inference time
)

domestic_indexer = StringIndexer(
    inputCol="Domestic",
    outputCol="domestic_index",
    handleInvalid="skip"
)

# VectorAssembler merges individual columns into a single feature vector
assembler = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index"],
    outputCol="features"
)
```

**What does `handleInvalid="skip"` mean?** If the test set contains a crime type that never appeared in training data, `StringIndexer` would normally throw an error. With `"skip"`, those rows are simply excluded from the output. An alternative is `"keep"`, which assigns a new index to unseen values.

### Step 2: Split Data

```python
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set:  {train_df.count():,} rows ({train_df.count()/df.count()*100:.1f}%)")
print(f"Test set:      {test_df.count():,} rows ({test_df.count()/df.count()*100:.1f}%)")

# Cache the training set — it will be read many times during fitting
train_df.cache()
print("Training data cached.")
```

**Why `seed=42`?** The random split must be reproducible. Using the same seed guarantees the same train/test split every time you run the code, which makes results comparable across experiments.

### Step 3: Inspect the Feature Vector (Optional but Educational)

Apply transformers manually to see what the data looks like before and after:

```python
# Fit and transform just to visualize
fitted_crime_indexer = crime_indexer.fit(train_df)
sample = fitted_crime_indexer.transform(train_df.limit(5))
sample.select("Primary Type", "crime_index").show()

# Check: how many unique crime types are there?
print(f"Unique crime types: {len(fitted_crime_indexer.labels)}")
print(f"First 5 labels: {fitted_crime_indexer.labels[:5]}")
```

### Questions — Part 2

1. `StringIndexer` assigns index 0 to the most frequent category. Which crime type gets index 0? Check using the code above.
2. Why must we fit `StringIndexer` **only on training data** and then apply to test data? What goes wrong if we fit on the combined dataset?
3. `VectorAssembler` creates a **dense** or **sparse** vector depending on the data. After assembling, run `train_df.select("features").show(3, truncate=False)`. Is the output dense or sparse? Why?
4. We chose `["District", "crime_index", "Hour", "domestic_index"]` as features. Propose **two additional features** you could extract from the existing columns that might improve predictions. Justify your choices.
5. *(Challenge)* Instead of `StringIndexer`, you could use `OneHotEncoder` after indexing. What is the difference between the two approaches? Under what conditions is one-hot encoding preferred over label encoding for categorical features?

---

## Part 3: Random Forest Classifier

**Goal:** Train a Random Forest, evaluate it properly, and interpret its feature importances.

### Why Random Forest?

A single decision tree is fast but tends to **overfit** — it memorizes the training data rather than learning general patterns. A Random Forest builds **many trees** (100 in our case), each trained on a random subset of data and a random subset of features. The final prediction is the **majority vote** across all trees. This ensemble approach dramatically reduces overfitting.

```
Tree 1 (random subset of data + features) → prediction A
Tree 2 (different random subset)           → prediction B
Tree 3 (different random subset)           → prediction C
...
Final prediction = majority vote of A, B, C, ...
```

### Step 1: Define and Train

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import time

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,     # number of trees in the ensemble
    maxDepth=5,       # max depth of each tree (controls overfitting)
    seed=42
)

# Bundle all stages into a Pipeline
pipeline_rf = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, rf])

print("Training Random Forest...")
t = time.time()
model_rf = pipeline_rf.fit(train_df)
rf_train_time = time.time() - t
print(f"Training time: {rf_train_time:.2f}s")
```

### Step 2: Evaluate

```python
predictions_rf = model_rf.transform(test_df)

# See what the model output columns look like
predictions_rf.select("label", "prediction", "probability").show(5, truncate=False)
# "probability" is a vector: [P(no arrest), P(arrest)]
# "prediction" is the class with the higher probability

binary_eval = BinaryClassificationEvaluator(labelCol="label")
mc_eval     = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

auc_rf      = binary_eval.evaluate(predictions_rf)
acc_rf      = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "accuracy"})
f1_rf       = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "f1"})
recall_rf   = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "weightedRecall"})
prec_rf     = mc_eval.evaluate(predictions_rf, {mc_eval.metricName: "weightedPrecision"})

print(f"AUC-ROC:   {auc_rf:.4f}")
print(f"Accuracy:  {acc_rf:.4f}")
print(f"F1 Score:  {f1_rf:.4f}")
print(f"Recall:    {recall_rf:.4f}")
print(f"Precision: {prec_rf:.4f}")
```

### Step 3: Confusion Matrix

A confusion matrix shows **all four outcomes** of a binary prediction:

```
                 Predicted: No Arrest   Predicted: Arrest
Actual: No Arrest      TN (correct)        FP (false alarm)
Actual: Arrest         FN (missed)         TP (correct)
```

```python
print("\n=== Confusion Matrix (Random Forest) ===")
print("label=0: No Arrest | label=1: Arrest")
predictions_rf.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
```

Record the four cells (TN, FP, FN, TP) from the output.

### Step 4: Feature Importances

```python
rf_model = model_rf.stages[-1]   # the fitted RandomForestModel is the last stage

feature_names = ["District", "crime_index", "Hour", "domestic_index"]
importances   = rf_model.featureImportances.toArray()

print("\n=== Feature Importances (Random Forest) ===")
for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
    bar = "█" * int(imp * 50)
    print(f"  {name:<20} {imp:.4f}  {bar}")
```

Feature importance measures how much each feature **reduces prediction uncertainty** across all trees. A higher value means the feature is used more often at earlier (more informative) decision points.

### Questions — Part 3

1. Look at the `probability` column output. It contains a vector like `[0.72, 0.28]`. What does each value mean? At what threshold does the model predict "Arrest"?
2. The model achieves high accuracy but a low F1 for the "Arrest" class. Explain what this means in practical terms, using the confusion matrix values.
3. Which feature has the highest importance? Does this match your intuition from the data exploration in Part 1? Why or why not?
4. If you increase `maxDepth` from 5 to 20, what would likely happen to: (a) training accuracy, (b) test accuracy, (c) training time? What is this phenomenon called?
5. The model outputs a `probability` column, not just a prediction. How could a police department use the probability score differently from the binary prediction? Give a concrete operational example.
6. *(Challenge)* Random Forest gives "feature importances" but not individual prediction explanations. What technique (not in MLlib but commonly used) would you use to explain *why* the model made a specific prediction for a single crime record?

---

## Part 4: Logistic Regression Classifier

**Goal:** Train a simpler linear model and compare it with the Random Forest.

### Why Logistic Regression?

Despite the name, Logistic Regression is a **classification** algorithm, not regression. It models the **probability** that a record belongs to class 1 (arrested) as:

$$P(\text{arrest}) = \frac{1}{1 + e^{-(w_0 + w_1 x_1 + w_2 x_2 + \ldots)}}$$

Where $w_i$ are learned weights (coefficients) and $x_i$ are the input features. The sigmoid function squashes the output to a value between 0 and 1.

**Strengths:** Fast to train, easy to interpret (positive coefficient = feature increases arrest probability), rarely overfits.  
**Weakness:** Assumes a linear decision boundary — cannot capture complex interactions between features.

### Step 1: Define and Train

```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,      # maximum gradient descent iterations
    regParam=0.01     # L2 regularisation strength (prevents overfitting)
)

pipeline_lr = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, lr])

print("Training Logistic Regression...")
t = time.time()
model_lr = pipeline_lr.fit(train_df)
lr_train_time = time.time() - t
print(f"Training time: {lr_train_time:.2f}s")
```

**What is `regParam`?** Regularisation adds a penalty for large coefficient values, preventing the model from becoming too sensitive to any single feature. `regParam=0.01` is a mild penalty — higher values (e.g., 1.0) force simpler models.

### Step 2: Evaluate

```python
predictions_lr = model_lr.transform(test_df)

auc_lr  = binary_eval.evaluate(predictions_lr)
acc_lr  = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "accuracy"})
f1_lr   = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "f1"})
prec_lr = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "weightedPrecision"})
rec_lr  = mc_eval.evaluate(predictions_lr, {mc_eval.metricName: "weightedRecall"})

print(f"AUC-ROC:   {auc_lr:.4f}")
print(f"Accuracy:  {acc_lr:.4f}")
print(f"F1 Score:  {f1_lr:.4f}")
print(f"Recall:    {rec_lr:.4f}")
print(f"Precision: {prec_lr:.4f}")

# Inspect learned coefficients
lr_model = model_lr.stages[-1]
coefficients = lr_model.coefficients.toArray()
feature_names = ["District", "crime_index", "Hour", "domestic_index"]
print("\n=== Logistic Regression Coefficients ===")
for name, coef in zip(feature_names, coefficients):
    direction = "↑ increases arrest probability" if coef > 0 else "↓ decreases arrest probability"
    print(f"  {name:<20} {coef:+.4f}  {direction}")
```

### Step 3: Confusion Matrix — Logistic Regression

```python
print("\n=== Confusion Matrix (Logistic Regression) ===")
predictions_lr.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
```

### Questions — Part 4

1. Logistic Regression converged in `maxIter=100` iterations. What does "convergence" mean in gradient descent? How would you verify it actually converged and didn't just stop at the iteration limit?
2. Which coefficient has the largest positive value? Translate this into a plain-English statement about what the model learned.
3. Logistic Regression assumes features contribute *linearly* and *independently*. Is this a reasonable assumption for crime prediction? Give a concrete example of a real interaction effect that linear models cannot capture.
4. `regParam` controls regularisation. What happens if you set it to 0? To 10? Which is more likely to overfit, and which is more likely to underfit?

---

## Part 5: Model Comparison and Analysis

**Goal:** Decide which model is better, and for what purpose.

### Step 1: Full Comparison Table

Fill in the table with values you recorded above:

| Metric | Random Forest | Logistic Regression | Better Model |
|--------|:------------:|:-------------------:|:------------:|
| AUC-ROC | | | |
| Accuracy | | | |
| F1 Score | | | |
| Weighted Recall | | | |
| Weighted Precision | | | |
| Training Time (s) | | | |
| Most Interpretable? | Partially | Yes | |

### Step 2: Confusion Matrix Comparison

|  | RF: Predicted 0 | RF: Predicted 1 | LR: Predicted 0 | LR: Predicted 1 |
|--|:---------------:|:---------------:|:---------------:|:---------------:|
| Actual 0 (TN / FP) | | | | |
| Actual 1 (FN / TP) | | | | |

### Thinking About Real-World Metrics

In predictive policing, the costs of errors are asymmetric:
- **False Negative** (FN): Model predicts no arrest, but the suspect actually gets arrested → police may be underprepared.
- **False Positive** (FP): Model predicts arrest, but the suspect is released → unnecessary resource allocation, potential bias implications.

Which error is more costly depends on the operational context — and this should drive metric selection.

### Questions — Part 5

1. Based on your confusion matrices, which model produces fewer **false negatives**? Why might minimising false negatives be more important than maximising accuracy in this application?
2. AUC-ROC is "threshold-independent." What does this mean? If you lower the prediction threshold from 0.5 to 0.3 (predict "arrest" whenever probability > 0.3), how do you expect precision and recall to change?
3. Random Forest took longer to train. In a production system that needs to retrain the model daily on new crime data, is training speed a critical factor? If so, which model would you choose?
4. Both models struggled more with the minority class (arrests). List **two concrete techniques** to handle class imbalance in Spark MLlib and briefly explain how each works.
5. *(Ethics)* Suppose the model learns that certain districts are strongly predictive of arrests. How might using district as a feature introduce or reinforce **algorithmic bias**? What steps should be taken before deploying such a model operationally?

---

## Bonus Exercises

These exercises extend the lab with real engineering and ML concepts.

### Bonus 1: Add More Features

```python
# Add Year — does including historical trend help?
assembler_v2 = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index", "Year"],
    outputCol="features"
)
# Rebuild and retrain your pipeline with assembler_v2
# Compare AUC before and after
```

**Question:** Does adding `Year` improve performance? Why might temporal features help or hurt?

### Bonus 2: Gradient Boosted Trees (GBT)

```python
from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=20, maxDepth=5, seed=42)
pipeline_gbt = Pipeline(stages=[crime_indexer, domestic_indexer, assembler, gbt])
model_gbt = pipeline_gbt.fit(train_df)
predictions_gbt = model_gbt.transform(test_df)
print(f"GBT AUC: {binary_eval.evaluate(predictions_gbt):.4f}")
```

**Question:** How does GBT differ from Random Forest? (Hint: trees are built sequentially, not independently.)

### Bonus 3: Save and Load the Model

```python
# Save the winning model to HDFS
model_rf.save("hdfs:///models/arrest_predictor_rf_v1")

# Reload it in a fresh session
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load("hdfs:///models/arrest_predictor_rf_v1")
loaded_predictions = loaded_model.transform(test_df.limit(100))
loaded_predictions.select("label", "prediction", "probability").show(5)
```

**Question:** Why is saving a model to HDFS (not local disk) important in a distributed cluster environment?

---

## Reflection Questions

Answer these after completing all parts. They require synthesising ideas across the full lab.

1. We built the pipeline in the order: `StringIndexer → VectorAssembler → Classifier`. Could you swap the order of `StringIndexer` and `VectorAssembler`? Why or why not?
2. You want to tune `maxDepth` (values: 3, 5, 10) and `numTrees` (values: 50, 100) for the Random Forest. How many total model training runs does this require? What is the name of this technique, and how would you implement it in Spark MLlib?
3. Suppose you must predict arrests in **real-time** as crimes are reported (latency < 100 ms per prediction). Would you use Spark MLlib for inference? If not, what would you use and why?
4. A model that achieves 80% accuracy on this dataset sounds impressive. Compute the accuracy of a trivial baseline model that always predicts "no arrest." What does this tell you about using accuracy as a standalone metric?

---

## Deliverables

Submit a report (PDF or Markdown) containing:

1. Completed comparison tables for all parts.
2. Written answers to **all** questions (Parts 1–5 + Reflection + any Bonus you attempted).
3. Confusion matrices for both models with the four values labeled (TN, FP, FN, TP).
4. Feature importance bar chart (copy the ASCII output or screenshot).
5. Your PySpark notebook (`.ipynb` or `.py`) committed to your team's GitHub repo — include the link.

---

## Grading

| Component | Points |
|-----------|:------:|
| Part 1: Data exploration + 5 questions | 15 |
| Part 2: Feature engineering + 5 questions | 15 |
| Part 3: Random Forest — metrics + confusion matrix + feature importances + 6 questions | 25 |
| Part 4: Logistic Regression — metrics + coefficients + 4 questions | 20 |
| Part 5: Model comparison + 5 questions | 15 |
| Reflection questions (4 questions) | 10 |
| **Total** | **100** |
