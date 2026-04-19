# SE446 Review Questions --- Weeks 07, 08, 09: Apache Spark & Spark MLlib

**Course:** SE446 Big Data Engineering  
**Instructor:** Prof. Anis Koubaa --- Alfaisal University  
**Scope:** Week 07 (Spark Fundamentals & DataFrames), Week 08 (Cluster Deployment & Tuning), Week 09 (Spark MLlib)  
**Date:** Spring 2026

---

## Section A: General Review / Conceptual Questions

These questions test your recall and understanding of core Spark concepts.

---

**A1.** What does RDD stand for? List and briefly explain its three key properties.

<details>
<summary>Answer</summary>

**Resilient Distributed Dataset.**

1. **Distributed** --- Data is split into partitions spread across multiple worker nodes in the cluster.
2. **Resilient** --- Fault tolerant via *lineage*: Spark records the sequence of transformations and can recompute lost partitions without replicating data.
3. **Lazy** --- Transformations build a logical plan (DAG) but nothing executes until an *action* is called (e.g., `.count()`, `.collect()`).

</details>

---

**A2.** Explain the difference between a **transformation** and an **action** in Spark. Give two examples of each.

<details>
<summary>Answer</summary>

- **Transformation** (lazy): Creates a new RDD/DataFrame from an existing one. Does *not* trigger execution.  
  Examples: `map()`, `filter()`, `groupBy()`, `select()`, `join()`

- **Action** (eager): Triggers the execution of the DAG and returns a result to the driver or writes to storage.  
  Examples: `count()`, `collect()`, `show()`, `saveAsTextFile()`, `write.parquet()`

</details>

---

**A3.** Why is Apache Spark significantly faster than MapReduce for iterative algorithms such as machine learning?

<details>
<summary>Answer</summary>

MapReduce writes intermediate results to HDFS after every Map and Reduce stage. For a 10-iteration ML algorithm, this means 20 HDFS read/write cycles (serialize, compress, replicate x3 each time).

Spark keeps intermediate data **in memory (RAM)** as RDDs. Iterative algorithms re-read the same data without touching disk, making Spark **10--100x faster** for iterative workloads.

</details>

---

**A4.** What is the role of each of the following Spark components?
- (a) Driver Program  
- (b) Cluster Manager  
- (c) Executor  
- (d) Task

<details>
<summary>Answer</summary>

- **(a) Driver Program** --- Hosts SparkContext; builds the DAG of stages; schedules tasks; collects results.
- **(b) Cluster Manager** --- Allocates resources (containers/JVMs) on the cluster (YARN, Standalone, Kubernetes).
- **(c) Executor** --- A JVM process on a worker node that runs tasks and holds cached data. Stays alive for the entire application.
- **(d) Task** --- The smallest unit of work. Each task processes one partition on one core of an executor.

</details>

---

**A5.** What is the difference between `SparkContext` and `SparkSession`? When do you use each?

<details>
<summary>Answer</summary>

- **SparkContext** is the original entry point for RDD-based programming (low-level API).
- **SparkSession** is the newer, unified entry point for structured APIs (DataFrames, Spark SQL, MLlib). It internally wraps a SparkContext (accessible via `spark.sparkContext`).

**Rule:** Use `SparkSession` for all modern Spark work. `SparkContext` is only needed for raw RDD operations.

</details>

---

**A6.** List three advantages of DataFrames over RDDs.

<details>
<summary>Answer</summary>

1. **Schema** --- DataFrames have named, typed columns (like a SQL table). RDDs are opaque Python objects with no schema.
2. **Catalyst Optimizer** --- DataFrame operations are optimized automatically (predicate pushdown, column pruning, constant folding). RDD code with Python lambdas cannot be optimized.
3. **Conciseness** --- The same query takes ~7 lines with DataFrames vs ~14 lines with RDDs, with no manual CSV parsing.

</details>

---

**A7.** Name and explain three optimizations that the Catalyst optimizer applies to DataFrame queries.

<details>
<summary>Answer</summary>

1. **Predicate Pushdown** --- Filters are moved as early as possible (e.g., before a join) to reduce the amount of data processed.
2. **Column Pruning** --- Only the columns referenced in the query are read from the data source; the rest are skipped.
3. **Constant Folding** --- Constant expressions are evaluated at compile time rather than for every row.
4. *(Bonus)* **Partition Pruning** --- Irrelevant Parquet/ORC partitions are skipped entirely.

</details>

---

**A8.** What is the difference between `createOrReplaceTempView` and `createOrReplaceGlobalTempView`?

<details>
<summary>Answer</summary>

- `createOrReplaceTempView("name")` --- Creates a **session-scoped** temporary view. It is lost when the SparkSession ends. Only visible within that session.
- `createOrReplaceGlobalTempView("name")` --- Creates a **global** temporary view visible to all SparkSessions in the same application.

</details>

---

**A9.** Compare `client` and `cluster` deploy modes in `spark-submit`. When should you use each?

<details>
<summary>Answer</summary>

| Aspect | Client Mode | Cluster Mode |
|--------|-------------|--------------|
| Driver location | On your machine (the submitter) | Inside the YARN cluster (ApplicationMaster) |
| `print()` output | Visible live in your terminal | Only in YARN logs |
| SSH disconnect | Job dies | Job continues |
| Best for | Development & debugging | Production & long-running jobs |

**Rule:** Use `client` mode during development (to see print output). Switch to `cluster` mode for final submission or unattended runs.

</details>

---

**A10.** Explain what happens when you call `df.cache()`. Does caching happen immediately?

<details>
<summary>Answer</summary>

`df.cache()` **marks** the DataFrame for caching in memory (storage level `MEMORY_ONLY`), but caching is **lazy** --- it does **not** happen immediately.

The data is actually cached on the **first action** (e.g., `df.count()`). That first action reads from HDFS and stores the result in RAM. Subsequent actions on the same DataFrame read from RAM instead of re-reading HDFS.

</details>

---

**A11.** What is the difference between `repartition(n)` and `coalesce(n)`?

<details>
<summary>Answer</summary>

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle | Full shuffle (expensive) | No shuffle (narrow dependency) |
| Direction | Can increase or decrease partitions | Can only **decrease** partitions |
| Distribution | Guarantees even distribution | Some partitions may be larger |
| Use case | Heavy computation, need more parallelism | Writing output files, reducing partitions |

</details>

---

**A12.** What is a broadcast join? When should you use it?

<details>
<summary>Answer</summary>

A **broadcast join** sends a small table (< 10 MB) to **every executor** in the cluster. Each executor then joins its local partitions of the large table with the small table **locally**, avoiding any shuffle of the large table.

**Use it when:** One table is small (a lookup/dimension table like district names, payment types, status codes) and the other is large. This eliminates the expensive shuffle of the large table across the network.

</details>

---

**A13.** In Spark MLlib, what is the difference between a **Transformer** and an **Estimator**?

<details>
<summary>Answer</summary>

- **Transformer** --- Takes a DataFrame, returns a new DataFrame. Method: `.transform(df)`. Parameters are fixed (no learning). Example: `VectorAssembler`.

- **Estimator** --- Takes a DataFrame, **learns** parameters from it, and produces a fitted Model (which is itself a Transformer). Method: `.fit(df)` -> Model. Example: `RandomForestClassifier`, `StringIndexer` (unfitted).

**Analogy:** A Transformer is a ruler (measures directly). An Estimator is a student (learns first, then measures).

</details>

---

**A14.** Why does Spark MLlib require all features to be in a single vector column? What tool creates this vector?

<details>
<summary>Answer</summary>

All Spark ML algorithms expect input as a **single vector column** called `features` (a `DenseVector` per row). This is different from scikit-learn where you pass a matrix of columns.

**`VectorAssembler`** combines multiple numeric columns (e.g., `District`, `crime_index`, `Hour`, `domestic_index`) into a single `features` vector column.

</details>

---

**A15.** When should you use `StringIndexer` alone vs. `StringIndexer` + `OneHotEncoder`?

<details>
<summary>Answer</summary>

- **StringIndexer alone** --- Sufficient for **tree-based models** (Random Forest, GBT). Trees split on values and do not assume any ordering between categories.

- **StringIndexer + OneHotEncoder** --- Required for **linear models** (Logistic Regression). Without one-hot encoding, LR treats index 2.0 as "more" than 1.0, creating a false ordinal relationship.

This is the same rule as in scikit-learn.

</details>

---

**A16.** What are the four file formats discussed in the course? Which one is recommended for Spark-to-Spark workflows and why?

<details>
<summary>Answer</summary>

| Format | Schema | Compression | Best For |
|--------|--------|-------------|----------|
| CSV | No | Poor | Data exchange |
| JSON | Self-describing | Poor | APIs |
| **Parquet** | **Embedded** | **Excellent** | **Analytics** |
| ORC | Embedded | Excellent | Hive |

**Parquet** is recommended because it is columnar (reads only needed columns), compressed (smaller I/O), and stores the schema (no `inferSchema` needed on read).

</details>

---

## Section B: Critical Thinking Questions

These questions test deeper reasoning, "why" and "what if" scenarios.

---

**B1.** A student writes the following RDD code and an equivalent DataFrame query. Both produce the same result, but the DataFrame version runs 3x faster. Explain **why** the Catalyst optimizer cannot help the RDD version.

```python
# RDD version
rdd.filter(lambda x: x != header) \
   .map(lambda x: x.split(",")) \
   .filter(lambda f: f[5] == "THEFT") \
   .map(lambda f: (f[5], 1)) \
   .reduceByKey(lambda a, b: a + b)

# DataFrame version
df.filter(col("Primary Type") == "THEFT") \
  .groupBy("Primary Type").count()
```

<details>
<summary>Answer</summary>

The Catalyst optimizer works by **inspecting and rewriting the logical plan**. With DataFrames, Spark sees the query structure (column names, filter predicates, aggregation) and can apply optimizations like:
- **Column pruning**: Only read the `Primary Type` column from the file.
- **Predicate pushdown**: Apply the `"THEFT"` filter at scan time.

With RDDs, Spark sees **opaque Python lambdas** (`lambda x: x.split(",")`, `lambda f: f[5] == "THEFT"`). It cannot inspect what these functions do, so it must execute them exactly as written --- reading all columns and processing all rows before filtering. **No optimization is possible on arbitrary Python functions.**

</details>

---

**B2.** A student's Spark job on a 4-core cluster takes 3 minutes. They check the Spark Web UI and see 200 tasks for a `groupBy` stage, with most tasks completing in 1ms. What is the problem, and how do they fix it?

<details>
<summary>Answer</summary>

**Problem:** The default `spark.sql.shuffle.partitions` is **200**. On a 4-core cluster with 10,000 rows, this means:
- 200 tasks / 4 cores = **50 rounds** of scheduling
- 10,000 rows / 200 = only 50 rows per task
- **Scheduling overhead far exceeds actual work**

**Fix:** Set `spark.sql.shuffle.partitions` to **8** (2x the number of cores):
```
--conf spark.sql.shuffle.partitions=8
```
This reduces to 2 rounds with 1,250 rows per task --- each core stays busy and scheduling overhead is minimal.

</details>

---

**B3.** The Chicago Crimes dataset has approximately 85% of records with `Arrest = False`. A student trains a model that achieves **85% accuracy** and concludes the model works well. What is wrong with this reasoning?

<details>
<summary>Answer</summary>

A model that **always predicts "No Arrest"** for every crime would achieve 85% accuracy simply by matching the majority class. This model catches **zero criminals** and is useless.

The student should evaluate with:
- **F1 Score** --- Balances precision and recall
- **AUC-ROC** --- Measures ranking quality (0.5 = random guessing)
- **Recall** --- Of actual arrests, how many did the model correctly identify?

**Lesson:** On imbalanced datasets, accuracy alone is misleading. Always check F1, AUC, precision, and recall.

</details>

---

**B4.** A student uses `inferSchema=True` when loading a large CSV file and notices the job takes twice as long as expected. Explain why and suggest a better approach.

<details>
<summary>Answer</summary>

`inferSchema=True` causes Spark to read the file **twice**:
1. **First pass** --- Scans all data to infer column types (int, string, boolean, etc.)
2. **Second pass** --- Actually loads the data with the inferred schema

**Better approach:** Define a `StructType` schema explicitly in code and pass it to `spark.read.csv(..., schema=my_schema)`. This skips the inference pass entirely, cutting load time roughly in half. For Parquet files, the schema is already embedded in the file --- no inference needed.

</details>

---

**B5.** A student writes the following code. Identify the **data leakage** problem and explain how to fix it using a Pipeline.

```python
indexer = StringIndexer(inputCol="Primary Type", outputCol="crime_index")
indexer_model = indexer.fit(df)       # fit on FULL data
df = indexer_model.transform(df)

train_df, test_df = df.randomSplit([0.8, 0.2])
model = rf.fit(train_df)
predictions = model.transform(test_df)
```

<details>
<summary>Answer</summary>

**Problem:** The `StringIndexer` is fitted on the **full dataset** (before splitting). This means the category-to-index mapping includes information from the test set, leaking test data into the training process. The model's evaluation metrics will be optimistically biased.

**Fix:** Use a Pipeline that fits all stages **only on the training data**:
```python
train_df, test_df = df.randomSplit([0.8, 0.2])

pipeline = Pipeline(stages=[
    crime_indexer,      # StringIndexer
    assembler,          # VectorAssembler
    rf                  # RandomForestClassifier
])

model = pipeline.fit(train_df)          # fits indexer on train ONLY
predictions = model.transform(test_df)  # applies same mapping to test
```

The Pipeline ensures the StringIndexer sees only training data during `.fit()`, and the same learned mapping is applied consistently during `.transform()` on test data.

</details>

---

**B6.** You call `df.cache()` and then immediately run `df.count()`. You notice the first `df.count()` is **not** faster than without caching. Why? When does the speedup appear?

<details>
<summary>Answer</summary>

`df.cache()` is **lazy** --- it only marks the DataFrame for caching. The **first action** (`df.count()`) still reads from HDFS because that action is what **triggers the caching** (read from HDFS + store in RAM).

The speedup appears on the **second and subsequent actions** (e.g., `df.groupBy(...).show()`, `df.filter(...).count()`), which now read from RAM instead of HDFS. 

Typical benchmark: Without cache 21s + 21s + 21s = 63s. With cache: 21s + 0.8s + 3.3s = 25s (2.5x speedup).

</details>

---

**B7.** Under what circumstances would you choose **scikit-learn** over **Spark MLlib** for a machine learning task?

<details>
<summary>Answer</summary>

Use scikit-learn when:
1. **Data fits in RAM** (< 5--10 GB) --- scikit-learn has no serialization/shuffle overhead
2. **Rapid prototyping** --- scikit-learn is faster to iterate with for exploration
3. **Need exotic algorithms** not available in MLlib (e.g., SVM with non-linear kernels, DBSCAN clustering, XGBoost with full options)
4. **Small team, quick iteration** --- no cluster setup required

**Key insight:** Distributed ML is **not** always better. Spark has overhead (serialization, shuffle, coordination). On small data, scikit-learn is faster. The decision is driven by **data size** and **infrastructure**.

</details>

---

**B8.** A student broadcasts a 500 MB table during a join. What will happen, and why?

<details>
<summary>Answer</summary>

Broadcasting a 500 MB table will cause an **OutOfMemoryError** on every executor. When you broadcast a table, Spark sends a **complete copy** to every executor's memory. If each executor has 1 GB of RAM and must hold 500 MB just for the broadcast table, there is very little memory left for actual data processing and task execution.

**Rule:** Only broadcast tables smaller than ~10 MB. For large-large table joins, use the default shuffle (Sort-Merge) join. Spark auto-broadcasts tables < 10 MB by default.

</details>

---

**B9.** A GBT model achieves an AUC of 0.92 on the Chicago crimes dataset, while Logistic Regression achieves only 0.72. What does this large gap tell you about the **nature of the data**?

<details>
<summary>Answer</summary>

The large gap tells you that the relationship between features and the label (Arrest) is **non-linear**. 

Logistic Regression can only learn a linear decision boundary. GBT (and Random Forest) can capture complex, non-linear interactions between features (e.g., "NARCOTICS in District 8 at night" has a different arrest pattern than "THEFT in District 1 at noon").

**If LR and RF had similar AUC**, the relationship would be approximately linear, and you should prefer LR (simpler, more interpretable). A large gap means tree-based models are capturing important non-linear patterns the linear model misses.

</details>

---

**B10.** Your Spark job's Stages tab shows one task taking 45 seconds while all other tasks in the same stage complete in 3 seconds. Diagnose the problem and suggest two possible fixes.

<details>
<summary>Answer</summary>

**Diagnosis:** This is a **data skew** problem. One partition contains significantly more data than the others (e.g., one key like "THEFT" dominates the dataset). The slow task (straggler) processes the oversized partition while all other cores sit idle waiting.

**Fixes:**
1. **Repartition with `repartition(n)`** --- Redistributes data more evenly across partitions using a full shuffle.
2. **Salting** --- Add a random suffix to the skewed key before groupBy (e.g., "THEFT_0", "THEFT_1", ...), aggregate, then remove the salt and aggregate again. This splits the large partition into smaller ones.
3. *(Alternative)* Increase the number of partitions to make each smaller overall.

</details>

---

## Section C: Problem-Solving / Engineering Questions

These questions require writing code, interpreting output, or performing calculations.

### Part 1 --- RDD Programming

---

**C1.** Given the Chicago Crimes CSV file on HDFS, write RDD code to:
1. Load the file with `sc.textFile()`
2. Skip the header line
3. Count the total number of crime records (excluding the header)

<details>
<summary>Answer</summary>

```python
from pyspark import SparkContext

sc = SparkContext("yarn", "CrimeCountRDD")

rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()
data_rdd = rdd.filter(lambda line: line != header)

total = data_rdd.count()
print(f"Total crimes: {total}")

sc.stop()
```

**Key points:**
- `textFile()` returns an RDD of strings (one per line)
- `first()` grabs the header, then `filter()` removes it
- `count()` is an action --- it triggers execution

</details>

---

**C2.** Using the RDD API, write code to find the **top 5 crime types by frequency**. Assume the CSV has columns: `ID, Date, Block, Primary Type, Description, ...` where `Primary Type` is at index 5 (0-based).

<details>
<summary>Answer</summary>

```python
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()

result = rdd.filter(lambda x: x != header) \
    .map(lambda x: x.split(",")) \
    .filter(lambda f: len(f) > 5) \
    .map(lambda f: (f[5], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)

for crime_type, count in result:
    print(f"{crime_type}: {count}")
```

**Step-by-step breakdown:**
1. `filter(lambda x: x != header)` --- skip header
2. `map(lambda x: x.split(","))` --- split each line into fields
3. `filter(lambda f: len(f) > 5)` --- skip malformed rows
4. `map(lambda f: (f[5], 1))` --- create (crime_type, 1) pairs
5. `reduceByKey(lambda a, b: a + b)` --- sum counts per type (triggers shuffle)
6. `sortBy(...)` --- sort descending by count
7. `take(5)` --- action: return top 5 to driver

</details>

---

**C3.** Using the RDD API, write code to compute the **arrest rate** (percentage of crimes that led to arrest). Assume `Arrest` is at column index 8 and has values `"true"` or `"false"`.

<details>
<summary>Answer</summary>

```python
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()

data_rdd = rdd.filter(lambda x: x != header) \
    .map(lambda x: x.split(",")) \
    .filter(lambda f: len(f) > 8)

total = data_rdd.count()
arrests = data_rdd.filter(lambda f: f[8].strip().lower() == "true").count()

arrest_rate = (arrests / total) * 100
print(f"Arrest rate: {arrest_rate:.2f}%")
```

**Note:** With RDDs, you must manually parse CSV fields, handle case sensitivity, and strip whitespace. DataFrames handle all this automatically with `inferSchema`.

</details>

---

**C4.** Using the RDD API, write code to count the **number of crimes per district**, and return the results as a dictionary to the driver.

<details>
<summary>Answer</summary>

```python
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()

crimes_per_district = rdd.filter(lambda x: x != header) \
    .map(lambda x: x.split(",")) \
    .filter(lambda f: len(f) > 3) \
    .map(lambda f: (f[3], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collectAsMap()

for district, cnt in sorted(crimes_per_district.items()):
    print(f"District {district}: {cnt}")
```

**Key points:**
- `collectAsMap()` is an action that returns the RDD of (key, value) pairs as a Python dictionary to the driver
- Alternative: `countByKey()` does the same in one call: `rdd.map(lambda f: (f[3], 1)).countByKey()`

</details>

---

**C5.** Identify the **transformations** and **actions** in the following RDD code. For each transformation, state whether it is **narrow** (no shuffle) or **wide** (shuffle).

```python
rdd = sc.textFile("hdfs:///data/crimes.csv")       # Line 1
rdd2 = rdd.filter(lambda x: x != header)            # Line 2
rdd3 = rdd2.map(lambda x: x.split(","))             # Line 3
rdd4 = rdd3.map(lambda f: (f[5], 1))                # Line 4
rdd5 = rdd4.reduceByKey(lambda a, b: a + b)         # Line 5
rdd6 = rdd5.sortBy(lambda x: x[1], ascending=False) # Line 6
result = rdd6.collect()                              # Line 7
```

<details>
<summary>Answer</summary>

| Line | Operation | Type | Narrow/Wide |
|------|-----------|------|-------------|
| 1 | `textFile()` | Load (creates RDD) | --- |
| 2 | `filter()` | **Transformation** | **Narrow** (each partition processed independently) |
| 3 | `map()` | **Transformation** | **Narrow** (element-wise, no data movement) |
| 4 | `map()` | **Transformation** | **Narrow** (element-wise) |
| 5 | `reduceByKey()` | **Transformation** | **Wide** (shuffle --- data with the same key must be co-located) |
| 6 | `sortBy()` | **Transformation** | **Wide** (shuffle --- requires global ordering across partitions) |
| 7 | `collect()` | **Action** | --- (triggers execution of the entire DAG) |

**Stages:** Lines 2-4 form **Stage 0** (narrow transforms). Line 5 triggers a shuffle boundary. Line 6 triggers another shuffle. So this job has **3 stages**.

</details>

---

### Part 2 --- DataFrame Operations

---

**C6.** Write DataFrame code to perform the following operations on the Chicago Crimes dataset:
1. Load the CSV with header and schema inference
2. Select only the columns `Primary Type`, `District`, `Arrest`
3. Filter for crimes where `Arrest` is `True`
4. Show the first 10 rows

<details>
<summary>Answer</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("FilterArrestsDF") \
    .getOrCreate()

df = spark.read.csv("hdfs:///data/chicago_crimes.csv",
                     header=True, inferSchema=True)

df.select("Primary Type", "District", "Arrest") \
  .filter(col("Arrest") == True) \
  .show(10)

spark.stop()
```

</details>

---

**C7.** Write DataFrame code to add a new column called `"ArrestLabel"` that contains `"Arrested"` when `Arrest` is `True` and `"Not Arrested"` otherwise. Then show 5 rows with columns `Primary Type`, `District`, `ArrestLabel`.

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import col, when

df = df.withColumn(
    "ArrestLabel",
    when(col("Arrest") == True, "Arrested")
    .otherwise("Not Arrested")
)

df.select("Primary Type", "District", "ArrestLabel").show(5)
```

**Key points:**
- `withColumn()` returns a **new** DataFrame (DataFrames are immutable)
- `when().otherwise()` is PySpark's equivalent of SQL `CASE WHEN`
- We reassign `df =` by convention, but the original DataFrame is not mutated

</details>

---

**C8.** Write DataFrame code to compute **multiple aggregations at once**: for each `District`, compute the total number of crimes and the arrest rate (average of the `Arrest` boolean cast to integer). Sort by district number.

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import col, count, avg

df.groupBy("District") \
  .agg(
      count("*").alias("total_crimes"),
      avg(col("Arrest").cast("int")).alias("arrest_rate")
  ) \
  .orderBy("District") \
  .show()
```

**Key points:**
- `groupBy().agg()` allows multiple aggregations in a single pass
- `cast("int")` converts boolean True/False to 1/0 so `avg()` computes the arrest rate
- `.alias()` renames the output column

</details>

---

**C9.** Write DataFrame code to perform an **inner join** between the crimes DataFrame and the following small lookup DataFrame, then show the result with district names instead of IDs.

```python
district_df = spark.createDataFrame([
    (1, "Central"), (2, "Wentworth"),
    (8, "Chicago Lawn"), (11, "Harrison"), (12, "Near West")
], ["DistrictID", "DistrictName"])
```

<details>
<summary>Answer</summary>

```python
from pyspark.sql.functions import broadcast

# Create lookup table
district_df = spark.createDataFrame([
    (1, "Central"), (2, "Wentworth"),
    (8, "Chicago Lawn"), (11, "Harrison"), (12, "Near West")
], ["DistrictID", "DistrictName"])

# Inner join (with broadcast since lookup is tiny)
result = df.join(
    broadcast(district_df),
    df["District"] == district_df["DistrictID"],
    "inner"
)

result.select("Primary Type", "DistrictName", "Arrest").show(10)
```

**Key points:**
- `broadcast()` sends the small lookup table to all executors (no shuffle of the large table)
- `"inner"` keeps only rows where District matches a DistrictID
- For a left join (keep all crimes even without a matching district), use `"left"` instead

</details>

---

**C10.** Write DataFrame code to:
1. Rename the column `"Primary Type"` to `"CrimeType"`
2. Drop the column `"Block"`
3. Print the schema to verify the changes

<details>
<summary>Answer</summary>

```python
df = df.withColumnRenamed("Primary Type", "CrimeType")
df = df.drop("Block")
df.printSchema()
```

**Key points:**
- `withColumnRenamed()` returns a new DataFrame with the renamed column
- `drop()` returns a new DataFrame without the specified column
- `printSchema()` displays column names, types, and nullable flags

</details>

---

**C11.** Write DataFrame code to inspect a DataFrame in three different ways: (a) print the schema, (b) show descriptive statistics (count, mean, stddev, min, max), and (c) check the number of partitions.

<details>
<summary>Answer</summary>

```python
# (a) Schema
df.printSchema()

# (b) Descriptive statistics
df.describe().show()

# (c) Number of partitions
print(f"Partitions: {df.rdd.getNumPartitions()}")
```

**Other useful inspection methods:**
- `df.dtypes` --- list of (column_name, type) tuples
- `df.columns` --- list of column names
- `df.count()` --- total row count (triggers a job!)
- `df.show(5, truncate=False)` --- first 5 rows without column truncation

</details>

---

**C12.** Write DataFrame code to save the result of a `groupBy` aggregation to HDFS in **Parquet** format, partitioned by the `District` column, using `overwrite` mode.

<details>
<summary>Answer</summary>

```python
result = df.groupBy("Primary Type", "District") \
    .agg(count("*").alias("crime_count"))

result.write \
    .partitionBy("District") \
    .mode("overwrite") \
    .parquet("hdfs:///output/crimes_by_district_parquet")
```

**Key points:**
- `partitionBy("District")` creates subdirectories like `District=1/`, `District=2/`, etc.
- This enables **partition pruning** on future reads: `spark.read.parquet(...)` with a filter on District will skip irrelevant directories
- `mode("overwrite")` deletes existing data before writing. Other modes: `append`, `ignore`, `error` (default)

</details>

---

### Part 3 --- Spark SQL Queries

---

**C13.** Register the crimes DataFrame as a temporary view and write a **Spark SQL** query to find the top 10 crime types by count.

<details>
<summary>Answer</summary>

```python
df.createOrReplaceTempView("crimes")

spark.sql("""
    SELECT `Primary Type`, COUNT(*) AS crime_count
    FROM crimes
    GROUP BY `Primary Type`
    ORDER BY crime_count DESC
    LIMIT 10
""").show()
```

**Key points:**
- Backticks around `Primary Type` are required because the column name contains a space
- `createOrReplaceTempView()` is session-scoped --- the view is lost when the SparkSession ends
- This produces the **exact same execution plan** as the equivalent DataFrame API code, thanks to the Catalyst optimizer

</details>

---

**C14.** Write a Spark SQL query to compute the **arrest rate (as a percentage)** for each crime type, showing only types with more than 50 occurrences, sorted by arrest rate descending.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT `Primary Type`,
           COUNT(*) AS total,
           ROUND(AVG(CAST(Arrest AS INT)) * 100, 1) AS arrest_pct
    FROM crimes
    GROUP BY `Primary Type`
    HAVING COUNT(*) > 50
    ORDER BY arrest_pct DESC
""").show()
```

**Key points:**
- `CAST(Arrest AS INT)` converts boolean to 0/1
- `AVG()` on 0/1 gives the proportion, `* 100` converts to percentage
- `HAVING` filters groups **after** aggregation (unlike `WHERE` which filters before)
- `ROUND(..., 1)` limits to 1 decimal place

</details>

---

**C15.** Write a Spark SQL query to find the **top 5 districts with the highest total crime count**, and show the district number and total count.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT District, COUNT(*) AS total
    FROM crimes
    GROUP BY District
    ORDER BY total DESC
    LIMIT 5
""").show()
```

</details>

---

**C16.** Write a Spark SQL query to show the **number of crimes per year**, sorted chronologically.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT Year, COUNT(*) AS total
    FROM crimes
    GROUP BY Year
    ORDER BY Year
""").show()
```

</details>

---

**C17.** Write a Spark SQL query that uses a **subquery** to find all crime types whose total count is **above the average count across all crime types**.

<details>
<summary>Answer</summary>

```python
spark.sql("""
    SELECT `Primary Type`, COUNT(*) AS cnt
    FROM crimes
    GROUP BY `Primary Type`
    HAVING COUNT(*) > (
        SELECT AVG(type_count)
        FROM (
            SELECT COUNT(*) AS type_count
            FROM crimes
            GROUP BY `Primary Type`
        )
    )
    ORDER BY cnt DESC
""").show()
```

**Key points:**
- The inner subquery computes the count per type, then the middle subquery computes the average of those counts
- `HAVING` compares each group's count against that average
- This is a more advanced SQL pattern --- same syntax as standard SQL

</details>

---

### Part 4 --- Three-Way Comparison (RDD vs DataFrame vs SQL)

---

**C18.** For each of the following tasks, write the solution in **all three APIs**: RDD, DataFrame API, and Spark SQL. The dataset is the Chicago Crimes CSV loaded on HDFS.

**Task:** Find all crimes of type `"NARCOTICS"` in District 11, and count them.

<details>
<summary>Answer</summary>

**RDD API:**
```python
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()

count = rdd.filter(lambda x: x != header) \
    .map(lambda x: x.split(",")) \
    .filter(lambda f: len(f) > 5 and f[5] == "NARCOTICS" and f[3] == "11") \
    .count()

print(f"Narcotics in District 11: {count}")
```

**DataFrame API:**
```python
from pyspark.sql.functions import col

count = df.filter(
    (col("Primary Type") == "NARCOTICS") & (col("District") == 11)
).count()

print(f"Narcotics in District 11: {count}")
```

**Spark SQL:**
```python
df.createOrReplaceTempView("crimes")

spark.sql("""
    SELECT COUNT(*) AS narcotics_count
    FROM crimes
    WHERE `Primary Type` = 'NARCOTICS'
      AND District = 11
""").show()
```

**Comparison:**
| | RDD | DataFrame | SQL |
|---|---|---|---|
| Lines | 6 | 4 | 5 |
| Optimizer | No | Yes (Catalyst) | Yes (Catalyst) |
| Readability | Low (manual parsing) | High | High (familiar SQL) |

</details>

---

**C19.** For each of the following tasks, write the solution in **all three APIs**: RDD, DataFrame API, and Spark SQL.

**Task:** Count the number of domestic vs non-domestic crimes, and show both counts.

<details>
<summary>Answer</summary>

**RDD API:**
```python
rdd = sc.textFile("hdfs:///data/chicago_crimes.csv")
header = rdd.first()

# Assume Domestic is at index 9
result = rdd.filter(lambda x: x != header) \
    .map(lambda x: x.split(",")) \
    .filter(lambda f: len(f) > 9) \
    .map(lambda f: (f[9].strip(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()

for domestic_flag, cnt in result:
    print(f"Domestic={domestic_flag}: {cnt}")
```

**DataFrame API:**
```python
from pyspark.sql.functions import count

df.groupBy("Domestic") \
  .agg(count("*").alias("total")) \
  .show()
```

**Spark SQL:**
```python
spark.sql("""
    SELECT Domestic, COUNT(*) AS total
    FROM crimes
    GROUP BY Domestic
""").show()
```

**Key takeaway:** The RDD version requires manual column indexing, string stripping, and a reduceByKey shuffle. The DataFrame and SQL versions are 2-3 lines each and benefit from Catalyst optimization (column pruning reads only the `Domestic` column).

</details>

---

### Part 5 --- Cluster Tuning, MLlib & Evaluation

---

**C20.** Write PySpark code to:
1. Create a SparkSession with app name `"CrimeAnalysis"`
2. Load `chicago_crimes.csv` from HDFS with header and schema inference
3. Count the number of crimes per `Primary Type`, sorted in descending order
4. Show the top 10 results
5. Write the result to HDFS as a Parquet file

<details>
<summary>Answer</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("CrimeAnalysis") \
    .getOrCreate()

df = spark.read.csv("hdfs:///data/chicago_crimes.csv",
                     header=True, inferSchema=True)

result = df.groupBy("Primary Type") \
  .agg(count("*").alias("crime_count")) \
  .orderBy(col("crime_count").desc())

result.show(10)

result.write.mode("overwrite") \
    .parquet("hdfs:///output/top_crimes_parquet")

spark.stop()
```

</details>

---

**C21.** Write the query from C20 using **Spark SQL** instead of the DataFrame API. Then explain why both versions produce the **exact same execution plan**.

<details>
<summary>Answer</summary>

```python
df.createOrReplaceTempView("crimes")

spark.sql("""
    SELECT `Primary Type`, COUNT(*) AS crime_count
    FROM crimes
    GROUP BY `Primary Type`
    ORDER BY crime_count DESC
    LIMIT 10
""").show()
```

**Why identical plans:** Both the DataFrame API and Spark SQL are translated into the same internal logical plan representation. The **Catalyst optimizer** processes both through the same pipeline: Parse -> Analyze -> Optimize -> Physical Plan. Since the logical operations are identical (scan, aggregate, sort), Catalyst produces the **same optimized physical plan** regardless of which API was used to express the query.

</details>

---

**C22.** Given the following `explain(True)` output, identify (a) which optimization was applied and (b) how many columns are actually read from the file:

```
== Optimized Logical Plan ==
Aggregate [Primary Type], [Primary Type, count(1) AS count]
+- Project [Primary Type]
   +- Relation[ID, Date, Primary Type, District, Arrest, ...] csv

== Physical Plan ==
HashAggregate(keys=[Primary Type], functions=[count(1)])
+- Exchange hashpartitioning(Primary Type, 200)
   +- HashAggregate(keys=[Primary Type], functions=[partial_count(1)])
      +- FileScan csv [Primary Type]
```

<details>
<summary>Answer</summary>

**(a) Column Pruning** --- The Optimized Logical Plan shows `Project [Primary Type]`, meaning Catalyst has pruned all columns except `Primary Type`. Even though the table has many columns (ID, Date, District, Arrest, etc.), only the one needed for the `groupBy` is projected.

**(b) Only 1 column** is actually read --- The Physical Plan shows `FileScan csv [Primary Type]`, confirming that at the file scan level, only the `Primary Type` column is read. All other columns are skipped.

Also visible: The `Exchange hashpartitioning` line indicates a **shuffle** between the two HashAggregate stages (partial local combine -> final global aggregate).

</details>

---

**C23.** Write the correct `spark-submit` command for the following scenario:

> You have a script `crime_analysis.py` that takes approximately 45 minutes. Your laptop may lose WiFi during the run. You have a 2-worker YARN cluster, each with 2 cores and 2 GB RAM. You want shuffle partitions set to 8.

<details>
<summary>Answer</summary>

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 2 \
    --executor-memory 1g \
    --executor-cores 2 \
    --driver-memory 512m \
    --conf spark.sql.shuffle.partitions=8 \
    crime_analysis.py
```

**Key decisions:**
- `--deploy-mode cluster` because WiFi may drop --- the Driver runs inside YARN and survives disconnection
- `--executor-memory 1g` (not 2g) to leave headroom for OS and YARN overhead
- `--conf spark.sql.shuffle.partitions=8` to avoid the 200-partition default on a 4-core cluster
- To check output afterward: `yarn logs -applicationId <app-id>`

</details>

---

**C24.** A student runs the following code **without caching**. How many times does Spark read the CSV file from HDFS?

```python
df = spark.read.csv("hdfs:///data/crimes.csv", header=True, inferSchema=True)
print(df.count())                          # Action 1
df.groupBy("Primary Type").count().show()  # Action 2
df.filter(col("Arrest") == True).count()   # Action 3
df.groupBy("District").count().show()      # Action 4
df.select("Year").distinct().show()        # Action 5
```

Now rewrite the code **with caching** and state how many HDFS reads occur.

<details>
<summary>Answer</summary>

**Without caching:** Spark reads from HDFS **5 times** (once per action). Spark is lazy and re-executes the entire DAG lineage from scratch for each action.

**With caching:**
```python
df = spark.read.csv("hdfs:///data/crimes.csv", header=True, inferSchema=True)
df.cache()

print(df.count())                          # Action 1: reads HDFS + stores in RAM
df.groupBy("Primary Type").count().show()  # Action 2: reads from RAM
df.filter(col("Arrest") == True).count()   # Action 3: reads from RAM
df.groupBy("District").count().show()      # Action 4: reads from RAM
df.select("Year").distinct().show()        # Action 5: reads from RAM

df.unpersist()  # free memory
```

**With caching:** Only **1 HDFS read** (on the first action). The remaining 4 actions read from RAM.

</details>

---

**C25.** Given a cluster with 2 workers, each having 2 cores (4 total cores):

(a) What is the ideal number of partitions for a DataFrame?  
(b) What should `spark.sql.shuffle.partitions` be set to?  
(c) If the DataFrame has only 1 partition, what is the problem?  
(d) If the DataFrame has 200 partitions, what is the problem?

<details>
<summary>Answer</summary>

**(a)** Ideal partitions = **2--4x total cores** = **8--16 partitions**.

**(b)** `spark.sql.shuffle.partitions` should be set to **8** (matching the ideal partition count for the cluster).

**(c)** With 1 partition: Only **1 core works** while **3 cores sit idle**. No parallelism. The job runs as if it were on a single machine.

**(d)** With 200 partitions: 200 / 4 cores = **50 rounds** of task scheduling. With 10,000 rows, each task processes only 50 rows --- the **scheduling overhead far exceeds the actual computation**. Many tasks will be empty or near-empty.

</details>

---

**C26.** Write a complete PySpark ML pipeline that:
1. Encodes `"Primary Type"` with StringIndexer
2. Encodes `"Domestic"` with StringIndexer
3. Assembles features `["District", "crime_index", "Hour", "domestic_index"]` into a vector
4. Trains a Random Forest with 100 trees and max depth 5
5. Evaluates with AUC-ROC and F1 score on test data

<details>
<summary>Answer</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

spark = SparkSession.builder.appName("CrimeML").getOrCreate()

# Load and prepare data
df = spark.read.csv("hdfs:///data/chicago_crimes.csv",
                     header=True, inferSchema=True)
df = df.withColumn("Hour",
    hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
df = df.select("District", "Primary Type", "Hour",
               "Domestic", "Arrest").dropna()
df = df.withColumn("label", col("Arrest").cast("integer"))

# Train/test split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()

# Define pipeline stages
crime_indexer = StringIndexer(
    inputCol="Primary Type", outputCol="crime_index",
    handleInvalid="skip")
domestic_indexer = StringIndexer(
    inputCol="Domestic", outputCol="domestic_index",
    handleInvalid="skip")
assembler = VectorAssembler(
    inputCols=["District", "crime_index", "Hour", "domestic_index"],
    outputCol="features")
rf = RandomForestClassifier(
    featuresCol="features", labelCol="label",
    numTrees=100, maxDepth=5, seed=42)

# Build and fit pipeline
pipeline = Pipeline(stages=[
    crime_indexer, domestic_indexer, assembler, rf
])
model = pipeline.fit(train_df)

# Predict and evaluate
predictions = model.transform(test_df)

binary_eval = BinaryClassificationEvaluator(labelCol="label")
auc = binary_eval.evaluate(predictions)

mc_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction")
f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})

print(f"AUC-ROC: {auc:.4f}")
print(f"F1 Score: {f1:.4f}")

train_df.unpersist()
spark.stop()
```

</details>

---

**C27.** Two workers each hold a partition of data. The current weights are **w = [0.5, 0.3]** and the learning rate is **eta = 0.1**.

| Worker | Data |
|--------|------|
| Worker 1 | x1=[1,2], y1=1; x2=[2,1], y2=0 |
| Worker 2 | x3=[3,1], y3=1; x4=[1,3], y4=0 |

The local gradients computed are:
- Worker 1: grad_L1 = [-0.045, 0.145]
- Worker 2: grad_L2 = [0.170, 0.330]

**(a)** Compute the aggregated gradient on the Driver.  
**(b)** Compute the updated weight vector.  
**(c)** What data travels over the network --- the raw data or the gradients? Why does this scale?

<details>
<summary>Answer</summary>

**(a)** Aggregated gradient:
```
grad_L = (1/2) * (grad_L1 + grad_L2)
       = (1/2) * ([-0.045 + 0.170, 0.145 + 0.330])
       = (1/2) * [0.125, 0.475]
       = [0.0625, 0.2375]
```

**(b)** Updated weights:
```
w_new = w - eta * grad_L
      = [0.5, 0.3] - 0.1 * [0.0625, 0.2375]
      = [0.5 - 0.00625, 0.3 - 0.02375]
      = [0.4938, 0.2763]
```

**(c)** Only the **gradient vectors** (and weight vectors) travel over the network --- NOT the raw data. In this example, only 2 floats per worker are sent to the driver. The raw data stays local on each worker.

**Why this scales:** As you add more workers, each processes its local partition and sends only a small gradient vector (same size regardless of data volume). Network traffic stays constant per iteration, while computation per worker decreases. This is why distributed gradient descent achieves near-linear speedup.

</details>

---

**C28.** Given the following confusion matrix from a crime arrest prediction model:

|  | Predicted: No Arrest | Predicted: Arrest |
|---|---|---|
| **Actual: No Arrest** | TN = 6800 | FP = 350 |
| **Actual: Arrest** | FN = 420 | TP = 1430 |

**(a)** Calculate Accuracy, Precision, Recall, and F1 Score.  
**(b)** In the context of crime prediction, is it worse to have high FP (false alarms) or high FN (missed arrests)? Justify your answer.  
**(c)** Which single metric would you prioritize, and why?

<details>
<summary>Answer</summary>

**(a)** Calculations:
```
Total = 6800 + 350 + 420 + 1430 = 9000

Accuracy  = (TP + TN) / Total = (1430 + 6800) / 9000 = 8230/9000 = 0.9144 (91.4%)
Precision = TP / (TP + FP) = 1430 / (1430 + 350) = 1430/1780 = 0.8034 (80.3%)
Recall    = TP / (TP + FN) = 1430 / (1430 + 420) = 1430/1850 = 0.7730 (77.3%)
F1        = 2 * P * R / (P + R) = 2 * 0.8034 * 0.7730 / (0.8034 + 0.7730) = 0.7879 (78.8%)
```

**(b)** **High FN (missed arrests) is worse.** A false negative means a criminal who should have been arrested walks free --- a direct public safety risk. A false positive (false alarm) wastes police resources (sending officers when no arrest is needed), which is costly but not dangerous. In crime prediction, **missing real criminals** is more consequential than false alarms.

**(c)** **Recall** should be prioritized, because it measures "of all actual arrests, how many did we catch?" In a safety-critical application, we want to minimize missed arrests (FN). However, F1 is a good balanced metric if we also want to avoid overwhelming police with too many false alarms. AUC-ROC is useful for overall model quality assessment.

</details>

---

*End of Review Questions*
