# Lab 02: Solving Real Problems with MapReduce (Chicago Crimes)

**Course**: SE446 - Big Data Engineering

**Objective**: Learn how to **translate** a real-world analytics question into a MapReduce algorithm. You will process a CSV dataset (Chicago Crimes) instead of simple text.

---

## 🎯 The Scenario

You are a data engineer for the Chicago Police Department. The Commissioner asks:

> "Which police district has the most arrests? We need to allocate more resources there."

This is not a simple word count. You have a CSV file, and you need to:

1.  Parse structured data (CSV).
2.  Filter for specific conditions (Arrest = True).
3.  Aggregate by a specific key (District).

---

## 🧠 Step 1: Designing the Algorithm

Before coding, we must design the **Key-Value** flow.

### The Question Breakdown

- **What are we counting?** Arrests.
- **How are we grouping?** By District.
- **Condition?** `Arrest == 'true'` (ignore non-arrests).

### The MapReduce Strategy

| Component   | Logic                                                                                                 | Sample Input       | Sample Output |
| :---------- | :---------------------------------------------------------------------------------------------------- | :----------------- | :------------ |
| **Mapper**  | 1. Parse CSV line<br>2. Check if `Arrest == true`<br>3. Extract `District`<br>4. Emit `(District, 1)` | `...8,true,011...` | `011  1`      |
| **Shuffle** | Group by District                                                                                     | `011 1`, `011 1`   | `011 [1, 1]`  |
| **Reducer** | Sum the counts                                                                                        | `011 [1, 1]`       | `011  2`      |

---

## 🛠️ Step 2: The Dataset

**File**: `chicago_crimes_sample.csv`

**Format**: CSV (Comma Separated Values)

**Schema**:
`ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location, Arrest, Domestic, Beat, District, ...`

**Example Row:**

```text
10224738,HY411648,09/05/2015 01:30:00 PM,043XX S WOOD ST,0486,BATTERY,DOMESTIC BATTERY SIMPLE,APARTMENT,false,true,0924,009,...
```

- **Arrest** is column index **8** (0-based count).
- **District** is column index **11**.

> **⚠️ Hazard**: CSV files can have commas _inside_ quotes (e.g., `"See, The"`). For this lab, we will assume a simple split by comma `,` is sufficient, but in production, use a CSV parser library.

---

## 📤 Step 2.1: Verify Shared Data

To save cluster resources, the dataset has already been uploaded to a **shared HDFS location** for you. You do **not** need to download or upload the CSV file yourself.

1.  **Connect to the Cluster**:

    ```bash
    ssh <your_username>@134.209.172.50
    ```

2.  **Verify you can find the data**:
    Check the shared `/data` directory:

    ```bash
    hdfs dfs -ls /data/
    ```

    **Expected Output:**

    ```text
    Found 2 items
    -rwxr-xr-x   1 akoubaa supergroup   ...  /data/chicago_crimes.csv
    -rwxr-xr-x   1 akoubaa supergroup   ...  /data/chicago_crimes_sample.csv
    ```

    > **Note**: You will use `/data/chicago_crimes_sample.csv` as your **Input** path in Step 5.

---

## 💻 Step 3: Writing the Mapper (`mapper_district.py`)

Create `mapper_district.py`.

**Pseudocode:**

```text
FUNCTION map(input_line):
    parts = SPLIT(input_line, ",")
    district = parts[11]
    arrest = parts[8]

    IF arrest IS "true":
        EMIT (district, 1)
```

```python
#!/usr/bin/env python3
import sys

# Index of relevant columns (check your CSV header!)
# Schema: ID, Case Number, Date, ..., Arrest(8), ..., District(11)
ARREST_IDX = 8
DISTRICT_IDX = 11

for line in sys.stdin:
    line = line.strip()

    # Skip empty lines
    if not line:
        continue

    parts = line.split(',')

    # Sanity Check: Ensure line has enough columns
    if len(parts) <= DISTRICT_IDX:
        continue

    # Extract fields
    arrest_status = parts[ARREST_IDX].lower() # 'true' or 'false'
    district = parts[DISTRICT_IDX]

    # LOGIC: Only emit if it IS an arrest
    if arrest_status == 'true':
        # Emit (Key=District, Value=1)
        print(f"{district}\t1")
```

### 🧪 Test Locally

Create a dummy `crime_test.csv`:

```text
1,A,Date,Block,001,THEFT,Desc,Loc,true,false,123,001
2,B,Date,Block,001,THEFT,Desc,Loc,false,false,123,001
3,C,Date,Block,001,BATTERY,Desc,Loc,true,false,123,002
```

Run command:

```bash
cat crime_test.csv | python3 mapper_district.py
```

**Expected Output** (Only 2 lines, because ID 2 was `false`):

```text
001     1
002     1
```

---

## 💻 Step 4: Writing the Reducer (`reducer_sum.py`)

This reducer is generic! A "Sum Reducer" simply sums values for a key. You can reuse this for _any_ counting job (WordCount, CrimeCount, etc.).

Create `reducer_sum.py`.

**Pseudocode:**

```text
FUNCTION reduce(key, value_list):
    total = 0
    FOR value IN value_list:
        total = total + value
    EMIT (key, total)
```

```python
#!/usr/bin/env python3
import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()

    try:
        key, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    if current_key == key:
        current_total += count
    else:
        if current_key:
            print(f"{current_key}\t{current_total}")
        current_key = key
        current_total = count

if current_key:
    print(f"{current_key}\t{current_total}")
```

### 🧪 Test Pipeline Locally

```bash
cat crime_test.csv | python3 mapper_district.py | sort | python3 reducer_sum.py
```

---

## 🚀 Step 5: Run on Cluster

1.  Connect to Master Node.
2.  Data is already at `/data/chicago_crimes_sample.csv` (or upload your own).
3.  Run the job:

```bash
mapred streaming \
    -files mapper_district.py,reducer_sum.py \
    -mapper "python3 mapper_district.py" \
    -reducer "python3 reducer_sum.py" \
    -input /data/chicago_crimes_sample.csv \
    -output /user/your_id/lab02/district_arrests
```

### 3.1 Monitoring Execution

While the job runs, look at the terminal output. You will see:

- **Job ID**: `job_16123456789_0001` (Unique ID for tracking).
- **Progress %**: `map 0% reduce 0%` -> `map 100% reduce 0%` -> `map 100% reduce 100%`.
- **Counters**: At the end, Hadoop summarizes "Map-Reduce Framework" stats (e.g., number of input records, output records).

### 3.2 View Results

To see the results stored in HDFS:

```bash
hdfs dfs -cat /user/your_id/lab02/district_arrests/part-00000
```

### 3.3 Expected Observations

You should see a list of District Codes and their arrest counts:

```text
001     532
002     489
003     612
...
025     890
```

- **Observation**: Districts with higher numbers indicate higher arrest activity.
- **Sanity Check**: If you see key `KeyError` or unexpected text, check your CSV column index in `mapper_district.py`.

---

## 🏆 Preparing for Milestone 1

The Milestone 1 asks you to count crimes by **Primary Type**.

- **How is this different?**
  - Instead of `District` (Col 11), you need `Primary Type` (Col 5).
  - Instead of filtering for `Arrest == true`, you might count ALL crimes (remove the `if` check).

By modifying `mapper_district.py` slightly (changing the column index and logic), you can solve the Milestone 1 problems!
