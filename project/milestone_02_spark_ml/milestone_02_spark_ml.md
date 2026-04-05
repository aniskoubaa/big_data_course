# Project Milestone 2: Chicago Crime Analytics with Spark + MLlib

**Due Date**: Sunday, Week 11, 23:59

**Group Size**: 2-5 Students (same groups as M1)

**Submission**: Single GitHub repository (contains both M1 and M2) via **AssessX Group Project**

## 1. Scenario: From Counting to Predicting

In Milestone 1, your team built a MapReduce pipeline to **count** crimes -- how many of each type, where they happen, how they trend over time. The Commander was impressed, but now he has a harder question:

> *"Can we **predict** whether a crime will result in an arrest **before** we dispatch officers?"*

Your mission is to upgrade your analytics from batch counting (MapReduce) to **in-memory analytics** (Spark) and **machine learning** (MLlib). You will:

1. **Reproduce** your M1 analyses using Spark DataFrames (faster, cleaner)
2. **Build an ML pipeline** to predict arrest outcomes
3. **Compare models** and recommend the best one
4. **Demonstrate** your work running both **locally** and on the **cluster**

---

## 2. Prerequisites & Local Setup

### Knowledge Prerequisites

- Completed **Week 7-9 lectures and labs** (Spark Core, Tuning, MLlib)
- The **W09B notebook** (`SE446_W09B_spark_mllib_lab.ipynb`) -- this is your reference implementation
- Familiar with `Pipeline`, `StringIndexer`, `VectorAssembler`, `RandomForestClassifier`

### Software Prerequisites (Install on Your Laptop)

You must have the following installed **before starting**:

| Software | Version | Install Command / Link |
|----------|---------|----------------------|
| **Python** | 3.8+ | [python.org/downloads](https://www.python.org/downloads/) |
| **Java (JDK)** | 11 or 17 | [adoptium.net](https://adoptium.net/) (Temurin JDK recommended) |
| **PySpark** | 3.5+ | `pip install pyspark` |
| **NumPy** | any | `pip install numpy` |
| **Jupyter** | any | `pip install jupyter` |
| **matplotlib** | any | `pip install matplotlib` (for Task 3 visualization) |

### Quick Verification

Run these commands to verify everything works:

```bash
# Check Python
python3 --version          # should print 3.8+

# Check Java (REQUIRED by Spark)
java -version              # should print 11.x or 17.x

# Check PySpark
python3 -c "import pyspark; print(pyspark.__version__)"  # should print 3.5+

# Test Spark runs
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()
print(f'Spark {spark.version} is working!')
spark.stop()
"
```

If `java -version` fails, Spark will not work. Install Java first.

### Common Setup Issues

| Problem | Solution |
|---------|----------|
| `java: command not found` | Install JDK 11 or 17 from [adoptium.net](https://adoptium.net/). On Mac: `brew install openjdk@17` |
| `JAVA_HOME is not set` | Add to your shell profile: `export JAVA_HOME=$(/usr/libexec/java_home)` (Mac) or set it to your JDK path |
| `ModuleNotFoundError: No module named 'pyspark'` | Run `pip install pyspark` |
| `Py4JJavaError` on Spark start | Java version mismatch. PySpark 3.5 needs Java 11 or 17 (not 8, not 21) |
| Spark logs flood the terminal | Add `spark.sparkContext.setLogLevel("WARN")` after creating SparkSession |

### Cluster Access

- Same credentials as M1 (SSH to the cluster)
- Dataset at `hdfs:///data/chicago_crimes.csv`
- If you lost your credentials, contact the instructor

### Google Colab Alternative

If you cannot install Java locally, use **Google Colab** as a fallback. The W09B notebook auto-installs PySpark on Colab. However, you still need cluster access for Tasks 10-11.

---

## 3. Dataset

Same dataset as M1:

- **Cluster**: `hdfs:///data/chicago_crimes.csv` (7M+ rows)
- **Local**: Use the data generator from the W09B lab notebook (10,000 rows)

**Schema reminder**:

| Column | Index | Type | Use |
|--------|-------|------|-----|
| ID | 0 | int | -- |
| Date | 2 | string | Extract Hour |
| Primary Type | 5 | string | Feature + analysis |
| Location Description | 7 | string | Analysis |
| Arrest | 8 | boolean | **Label** (predict this) |
| Domestic | 9 | boolean | Feature |
| District | 11 | int | Feature |
| Year | 17 | int | Analysis |

---

## 4. Required Tasks

### Phase A: Spark DataFrame Analytics (Reproduce M1 in Spark)

These tasks mirror your M1 MapReduce analyses. Same questions, but using Spark DataFrames and Spark SQL instead of mapper/reducer scripts.

#### Task 1: Crime Type Distribution (Spark DataFrame)

- **Same as M1 Task 2**, but using Spark:
```python
df.groupBy("Primary Type").count().orderBy(col("count").desc()).show()
```
- **Requirement**: Show the top 10 crime types with counts
- **Comparison**: Include your M1 MapReduce results side-by-side. Are the numbers identical? They should be.

#### Task 2: Location Hotspots (Spark SQL)

- **Same as M1 Task 3**, but using **Spark SQL**:
```python
df.createOrReplaceTempView("crimes")
spark.sql("""
    SELECT `Location Description`, COUNT(*) as total
    FROM crimes
    GROUP BY `Location Description`
    ORDER BY total DESC
    LIMIT 10
""").show()
```
- **Requirement**: Use `spark.sql()` (not DataFrame API) to demonstrate SQL-on-Spark

#### Task 3: Crime Trend Over Years (DataFrame + Visualization)

- **Same as M1 Task 4**, but add a simple visualization:
```python
yearly = df.groupBy("Year").count().orderBy("Year").toPandas()
# Plot using matplotlib (locally) or print table (on cluster)
```
- **Requirement**: Show crime count per year. On local mode, include a matplotlib line chart. On cluster, a printed table is sufficient.

#### Task 4: Arrest Rate Analysis (DataFrame)

- **Same as M1 Task 5**, plus a breakdown by crime type:
```python
# Overall arrest rate
# + arrest rate PER crime type (top 10)
```
- **Requirement**: Show both the overall rate and per-type rates. Which crime types have the highest/lowest arrest rates? Interpret the results.

---

### Phase B: Spark MLlib -- Arrest Prediction

Build a complete ML pipeline to predict whether a crime results in an arrest.

#### Task 5: Feature Engineering Pipeline

Build a Spark ML Pipeline with:
- `StringIndexer` for `Primary Type` and `Domestic`
- `VectorAssembler` to combine `District`, `crime_index`, `Hour`, `domestic_index` into a `features` vector
- Train/test split: 80/20 with `seed=42`

**Deliverable**: Show the `features` column for 5 sample rows. Explain what each position in the vector represents.

#### Task 6: Train and Evaluate Three Models

Train and evaluate **all three** classifiers:

| Model | Key Parameters |
|-------|---------------|
| Logistic Regression | `maxIter=100`, `regParam=0.01` |
| Random Forest | `numTrees=100`, `maxDepth=5` |
| GBT | `maxIter=50`, `maxDepth=5` |

For each model, report:
- AUC-ROC, Accuracy, F1 Score, Precision, Recall
- Confusion matrix (TN, FP, FN, TP)
- Training time

**Deliverable**: A comparison table with all three models side-by-side.

#### Task 7: Feature Importances & Interpretation

- Extract and display feature importances from the Random Forest model
- Answer: Which feature is most important? Does this match the arrest rate analysis from Task 4?
- Answer: Why does Logistic Regression perform worse than tree-based models on this data?

**Deliverable**: Feature importance bar chart (or ASCII table) + written interpretation.

#### Task 8: Hyperparameter Tuning with CrossValidator

Use `CrossValidator` + `ParamGridBuilder` to tune the Random Forest:

| Parameter | Values to Try |
|-----------|--------------|
| `numTrees` | 50, 100, 200 |
| `maxDepth` | 3, 5, 10 |

- Use 3-fold cross-validation with AUC-ROC as the metric
- Report which combination performed best
- Report the best model's AUC on the test set

**Deliverable**: CrossValidator results table + best model metrics.

---

### Phase C: Deployment Modes

You must demonstrate your code running in **three execution modes**:

#### Task 9: Local Execution

- Run the complete notebook (Phase A + B) on your laptop using `local[*]` mode
- Use the generated sample data (10,000 rows)
- **Evidence**: Screenshot or notebook output showing `Master: local[*]`

#### Task 10: Cluster Execution -- Client Mode

- SSH to the cluster and run the notebook using `--master yarn --deploy-mode client`
- Use the full dataset from HDFS (`hdfs:///data/chicago_crimes.csv`)
- **Evidence**: Screenshot showing `Master: yarn` and the real row count (7M+)

#### Task 11: Cluster Execution -- spark-submit

- Convert your ML pipeline (Phase B, Tasks 5-7) into a standalone Python script (`.py`)
- Submit it to the cluster using `spark-submit`:
```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 1g \
    --executor-cores 2 \
    m2_spark_ml.py
```
- **Evidence**: Full terminal output of the `spark-submit` command showing job completion and results

---

## 5. Work Distribution & Git Workflow

### One Student, One Task Rule

Assign tasks to members. Every member must commit code for their assigned tasks.

**Suggested distribution** (for a 4-person team):

| Member | Tasks | Phase |
|--------|-------|-------|
| Member A | Tasks 1-2 (DataFrame + SQL analytics) | A |
| Member B | Tasks 3-4 (Trends + arrest rate analysis) | A |
| Member C | Tasks 5-7 (ML pipeline + evaluation) | B |
| Member D | Tasks 8-11 (Tuning + deployment modes) | B+C |

### Code Authorship -- Put Your Name on Your Code

Every code cell or script section you write **must include a comment header** with your name and the task. This is how we verify who wrote what.

**Format for notebook cells:**

```python
# ============================================
# Task 5: Feature Engineering Pipeline
# Author: Ali Al-Rashid (ID: 12345)
# ============================================

crime_indexer = StringIndexer(
    inputCol="Primary Type",
    outputCol="crime_index",
    handleInvalid="skip"
)
# ... rest of your code
```

**Format for Python scripts (`m2_spark_ml.py`):**

```python
# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Group X
#
# Task 5-6: Ali Al-Rashid (ID: 12345)
# Task 7:   Sara Ahmad (ID: 67890)
# Task 8:   Omar Hassan (ID: 11111)
# ============================================
```

**Format for notebook markdown cells (before your code):**

```markdown
### Task 3: Crime Trend Over Years
**Author: Sara Ahmad (ID: 67890)**
```

If your name does not appear on any code, you will be treated as a non-contributor.

### Git Workflow

You can use **either** of these two workflows. Both are acceptable:

**Option A: Branch + Pull Request (recommended)**
1. Create a branch for your task: `git checkout -b task5-ali`
2. Write and commit your code to the branch
3. Push the branch: `git push origin task5-ali`
4. Open a **Pull Request** on GitHub to merge into `main`
5. Group leader (or another member) reviews and merges

**Option B: Branch + Merge locally**
1. Create a branch: `git checkout -b task3-sara`
2. Write and commit your code
3. Switch to main: `git checkout main`
4. Merge: `git merge task3-sara`
5. Push: `git push origin main`

**What matters:** Your GitHub username must appear in the commit history with meaningful code changes. One mega-commit at the end does not count.

---

## 6. Deliverables

### 1. Jupyter Notebook: `M2_Spark_ML_GroupX.ipynb`

A single notebook containing:
- All Tasks 1-8 with code, output, and written analysis
- Clear markdown headers for each task
- The 3-model comparison table
- Feature importance output
- CrossValidator results

### 2. Python Script: `m2_spark_ml.py`

Standalone script for Tasks 5-7 (Phase B), runnable via `spark-submit`.

### 3. GitHub README

Your repository `README.md` must include:
1. **Team members** (names + IDs)
2. **Executive summary** (2-3 sentences)
3. **M1 vs M2 comparison**: For Tasks 1-4, show side-by-side results (MapReduce vs Spark). Are the numbers the same? Which was faster/easier?
4. **ML results summary**: Best model, key metrics, interpretation
5. **Deployment evidence**: Screenshots of local, client mode, and spark-submit execution
6. **Member contributions**: Table showing who did what
7. **spark-submit terminal output**: Full copy-paste of the spark-submit execution log

### 4. Submission

- **One GitHub repository** per group for M2
- Submit via **AssessX Group Project** (link will be announced)
- Ensure the instructor (`akoubaa`) is a collaborator on the repo

**Repository structure:**
```
se446-m2-group-X/
├── README.md                     ← project report
├── M2_Spark_ML_GroupX.ipynb      ← main notebook
├── m2_spark_ml.py                ← standalone script for spark-submit
├── output/                       ← results, screenshots, evidence
└── docs/                         ← (optional) additional documentation
```

---

## 7. Evaluation Criteria

### Group Score (Project Quality)

| Component | Points | Details |
|-----------|:------:|---------|
| **Phase A**: Spark DataFrame Analytics (Tasks 1-4) | 20 | Correct results, Spark SQL usage, M1 comparison |
| **Phase B**: ML Pipeline (Tasks 5-7) | 30 | Pipeline correctness, 3 models evaluated, interpretation |
| **Phase B**: CrossValidator (Task 8) | 10 | Grid search, results table, best model selection |
| **Phase C**: Deployment Modes (Tasks 9-11) | 15 | Local + client + spark-submit evidence |
| **Report**: README completeness | 15 | All sections present, M1 vs M2 comparison |
| **Git**: Branches + PRs used | 10 | Proper Git workflow followed |
| **Total** | **100** | |

### Individual Grade Adjustment (GitHub Activity)

The group score above is the **base**. Each member's **individual grade** is then adjusted based on their **personal GitHub contribution**, analyzed automatically via AssessX:

| Contribution Level | Grade Modifier |
|-------------------|----------------|
| Strong contributor (meaningful commits, PRs, code authorship) | 100% of group score |
| Moderate contributor (some commits, participated in reviews) | 70-90% of group score |
| Minimal contributor (few or trivial commits) | 30-60% of group score |
| No commits / ghost member | **0%** (zero regardless of group score) |

**What counts as contribution:**
- Commits with meaningful code changes (not just whitespace or README edits)
- Pull requests authored or reviewed
- Issues created or resolved
- Code authorship visible in `git blame`

**What does NOT count:**
- A single "mega commit" at the end (suggests copy-paste, not development)
- Commits authored by another member pushed under your name
- Identical commits across multiple members

**The rule is simple: if your GitHub username does not appear in the commit history for substantive code, your individual grade is zero.**

---

## 8. Hints & Common Pitfalls

**1. Start with local mode.** Get everything working on your laptop with 10,000 generated rows before touching the cluster. Local debugging is 100x faster.

**2. The notebook auto-detects environment.** Use the same auto-detection pattern from the W09B lab notebook. One notebook, runs everywhere.

**3. Cache your training data.**
```python
train_df.cache()
```
ML algorithms are iterative. Without caching, Spark re-reads from HDFS on every tree/iteration.

**4. Spark-submit needs a self-contained script.** Your `.py` script must create its own SparkSession, load data, run the pipeline, and print results. It cannot rely on notebook-specific features (like `display()`).

**5. Column names with spaces.** The Chicago Crimes CSV has `Primary Type` (with a space). In Spark SQL, wrap it in backticks: `` `Primary Type` ``. In DataFrame API, use `col("Primary Type")`.

**6. Label column must be integer.** MLlib classifiers expect `labelCol` to contain integers (0/1), not booleans or strings. Cast with:
```python
df = df.withColumn("label", col("Arrest").cast("integer"))
```

**7. GBT is binary only.** Spark's `GBTClassifier` only supports 2 classes. This is fine for our arrest prediction (0 or 1).

**8. StringIndexer `handleInvalid`.** Set `handleInvalid="skip"` to avoid errors when the test set contains crime types not seen in training.

**9. Save model for evidence.** Save your best model to HDFS as proof:
```python
model.save("hdfs:///user/your_id/project/m2/best_model")
```

**10. M1 comparison is required.** You must show that your Spark results (Tasks 1-4) match your MapReduce results from M1. Same data, same numbers -- different technology.

---

## 9. AI Usage Policy

Same policy as M1:

- **Allowed**: Debugging errors, conceptual explanations, generating comments
- **Prohibited**: Generating entire pipeline/tasks from scratch
- **Penalty**: If you cannot explain your code during the in-class check, you receive a **Zero**
