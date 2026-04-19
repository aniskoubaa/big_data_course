# Lab 02 - Execution Report: Solving Real Problems with MapReduce

**Course**: SE446 - Big Data Engineering  
**Lab**: Intermediate MapReduce — Chicago Crime Arrests by District  
**Cluster**: Hadoop 3.4.1 (1 Master + 2 Workers)  
**Date Executed**: February 13, 2026  
**Account Used**: `test_student`

---

## Table of Contents

1. [Lab Objective](#1-lab-objective)
2. [Cluster Connection](#2-cluster-connection)
3. [Understanding the Dataset](#3-understanding-the-dataset)
4. [The MapReduce Algorithm](#4-the-mapreduce-algorithm)
5. [Step-by-Step Execution](#5-step-by-step-execution)
6. [Results & Interpretation](#6-results--interpretation)
7. [Full Execution Log](#7-full-execution-log)
8. [Preparing for Milestone 1](#8-preparing-for-milestone-1)
9. [Troubleshooting Guide](#9-troubleshooting-guide)

---

## 1. Lab Objective

The Chicago Police Commissioner asks:

> *"Which police district has the most arrests? We need to allocate more resources there."*

To answer this, we will:

1. **Parse** a CSV dataset (Chicago Crime Reports)
2. **Filter** for arrests only (`Arrest == true`)
3. **Count** arrests per police district
4. **Run** this analysis on a Hadoop cluster using MapReduce Streaming

This lab teaches you how to go from a real-world question to a working MapReduce job.

---

## 2. Cluster Connection

### 2.1 SSH into the Master Node

Open your terminal and connect to the cluster:

```bash
ssh your_username@134.209.172.50
```

When prompted, enter your password.

> **💡 Tip**: If your password contains `!`, enclose it in single quotes:  
> `ssh your_username@134.209.172.50` → then type: `'MyPass!word'`

### 2.2 Source the Hadoop Environment (CRITICAL)

**Every time you log in**, run this command:

```bash
source /etc/profile.d/hadoop.sh
```

Without this, commands like `mapred`, `hdfs`, and `yarn` will not work. You'll get:

```
bash: mapred: command not found
```

### 2.3 Verify the Dataset Exists

Check that the shared dataset is available on HDFS:

```bash
hdfs dfs -ls /data/
```

**Expected Output**:

```
Found 2 items
-rwxr-xr-x   2 hadoop hadoop  2007709341 2026-02-12 15:12 /data/chicago_crimes.csv
-rwxr-xr-x   2 hadoop hadoop     2387194 2026-02-12 15:47 /data/chicago_crimes_sample.csv
```

You will use `/data/chicago_crimes_sample.csv` (10,000 records) for this lab.

---

## 3. Understanding the Dataset

### 3.1 CSV Schema

The first line of the file is a header:

```
ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrest, Domestic, Beat, District, Ward, ...
```

**Column Index Reference** (0-based):

| Index | Column Name | Example Value | Used In |
|:-----:|:------------|:--------------|:--------|
| 0 | ID | `13311263` | — |
| 1 | Case Number | `JG503434` | — |
| 2 | Date | `07/29/2022 03:39:00 AM` | Milestone 1, Task 4 |
| 3 | Block | `023XX S TROY ST` | — |
| 4 | IUCR | `1582` | — |
| 5 | **Primary Type** | `THEFT` | **Milestone 1, Task 2** |
| 6 | Description | `OVER $500` | — |
| 7 | **Location Description** | `STREET` | **Milestone 1, Task 3** |
| 8 | **Arrest** | `true` / `false` | **This Lab + Milestone 1, Task 5** |
| 9 | Domestic | `true` / `false` | — |
| 10 | Beat | `1033` | — |
| 11 | **District** | `010` | **This Lab** |
| 12 | Ward | `25` | — |

### 3.2 Sample Row

```
13311263,JG503434,07/29/2022 03:39:00 AM,023XX S TROY ST,1582,OFFENSE INVOLVING CHILDREN,CHILD PORNOGRAPHY,RESIDENCE,true,false,1033,010,25,30,17,,,2022,04/18/2024 03:40:59 PM,,,
```

Reading this row:
- **Arrest** (index 8) = `true` → This person WAS arrested
- **District** (index 11) = `010` → This happened in District 10

---

## 4. The MapReduce Algorithm

### 4.1 Algorithm Design

Before writing any code, think about the **Key-Value** flow:

```
┌──────────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│       MAPPER         │     │     SHUFFLE      │     │     REDUCER      │
│                      │     │   (Hadoop does   │     │                  │
│  For each CSV line:  │     │    this for us)  │     │  For each key:   │
│  1. Parse columns    │────▶│                  │────▶│  Sum all counts  │
│  2. Check Arrest=true│     │  Group by key    │     │  Emit (key, sum) │
│  3. Emit (district,1)│     │  Sort by key     │     │                  │
└──────────────────────┘     └──────────────────┘     └──────────────────┘
```

**Concrete Example**:

| Stage | Input | Output |
|:------|:------|:-------|
| **Mapper** (line 1) | `...,true,...,010,...` | `010  1` |
| **Mapper** (line 2) | `...,false,...,010,...` | *(nothing — filtered out)* |
| **Mapper** (line 3) | `...,true,...,010,...` | `010  1` |
| **Mapper** (line 4) | `...,true,...,011,...` | `011  1` |
| **Shuffle** | `010 1`, `010 1`, `011 1` | `010 [1,1]`, `011 [1]` |
| **Reducer** | `010 [1,1]` | `010  2` |
| **Reducer** | `011 [1]` | `011  1` |

---

## 5. Step-by-Step Execution

### Step 1: Write the Scripts

#### `mapper_district.py`

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
    arrest_status = parts[ARREST_IDX].lower()  # 'true' or 'false'
    district = parts[DISTRICT_IDX]

    # LOGIC: Only emit if it IS an arrest
    if arrest_status == 'true':
        # Emit (Key=District, Value=1)
        print(f"{district}\t1")
```

**Key Points**:
- We read from `sys.stdin` (standard input) — this is how MapReduce feeds data to the mapper
- We use `\t` (tab) to separate key and value — this is the MapReduce convention
- We skip the header row implicitly (it won't match `arrest_status == 'true'`)
- We filter: only emit when `Arrest == true`

#### `reducer_sum.py`

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

**Key Points**:
- Hadoop **sorts** data between mapper and reducer — so all values for the same key arrive together
- We track `current_key` and accumulate its count
- When the key changes, we emit the previous key's total and start a new accumulator
- **Don't forget the last key!** (the `if current_key` after the loop)
- This reducer is **generic** — you'll reuse it for all Milestone 1 tasks

### Step 2: Test Locally (On Your Laptop)

Before uploading to the cluster, always test locally:

```bash
# Create a test file with 3 rows
echo "1,A,Date,Block,001,THEFT,Desc,APARTMENT,true,false,123,001,1,1,01" > crime_test.csv
echo "2,B,Date,Block,001,THEFT,Desc,STREET,false,false,123,001,1,1,01" >> crime_test.csv
echo "3,C,Date,Block,001,BATTERY,Desc,SIDEWALK,true,false,123,002,1,1,01" >> crime_test.csv
```

**Test the mapper alone**:

```bash
cat crime_test.csv | python3 mapper_district.py
```

**Expected output** (only 2 lines — row 2 was `false`, so it was filtered):

```
001     1
002     1
```

✅ **Verified on cluster** — Mapper correctly emitted 2 records and filtered out 1 non-arrest.

**Test the full pipeline** (mapper → sort → reducer):

```bash
cat crime_test.csv | python3 mapper_district.py | sort | python3 reducer_sum.py
```

**Expected output**:

```
001     1
002     1
```

✅ **Verified on cluster** — Full pipeline produces correct aggregated counts.

> **Why `sort`?** In real MapReduce, Hadoop sorts the mapper output before passing it to the reducer. When testing locally, we simulate this with the `sort` command.

### Step 3: Upload to the Cluster

From **your laptop** (not the cluster), upload both scripts:

```bash
scp mapper_district.py reducer_sum.py your_username@134.209.172.50:~/
```

Enter your password when prompted. Verify the files arrived:

```bash
# On the cluster after SSH:
ls -la ~/mapper_district.py ~/reducer_sum.py
```

### Step 4: Run the MapReduce Job

SSH into the cluster and run:

```bash
# 1. Source Hadoop environment
source /etc/profile.d/hadoop.sh

# 2. Submit the MapReduce job
mapred streaming \
    -files mapper_district.py,reducer_sum.py \
    -mapper "python3 mapper_district.py" \
    -reducer "python3 reducer_sum.py" \
    -input /data/chicago_crimes_sample.csv \
    -output /user/your_username/lab02/district_arrests
```

**Breaking down the command**:

| Flag | Meaning |
|:-----|:--------|
| `mapred streaming` | Run a streaming MapReduce job (Python scripts, not Java) |
| `-files mapper.py,reducer.py` | Ship these files to all worker nodes |
| `-mapper "python3 mapper.py"` | Command to run for each mapper |
| `-reducer "python3 reducer.py"` | Command to run for each reducer |
| `-input /data/...` | HDFS path to input dataset |
| `-output /user/.../` | HDFS path for results (must NOT exist yet) |

### Step 5: Monitor the Job

While running, you'll see progress updates:

```
2026-02-13 18:33:47 INFO: Running job: job_1770991083092_0013
2026-02-13 18:34:15 INFO: map 0% reduce 0%
2026-02-13 18:34:47 INFO: map 100% reduce 0%     ← All mappers done!
2026-02-13 18:35:05 INFO: map 100% reduce 100%   ← Reducer done!
2026-02-13 18:35:06 INFO: Job job_1770991083092_0013 completed successfully
```

✅ **This job completed in approximately 1 minute 22 seconds.**

### Step 6: View Results

```bash
hdfs dfs -cat /user/your_username/lab02/district_arrests/part-00000
```

---

## 6. Results & Interpretation

### 6.1 Full Output (22 Districts)

Running the job on `/data/chicago_crimes_sample.csv` (10,000 crime records) produced:

```
001     101
002     44
003     50
004     57
005     54
006     58
007     59
008     74
009     61
010     55
011     109
012     50
014     41
015     43
016     35
017     26
018     60
019     45
020     35
022     36
024     47
025     80
```

### 6.2 Top 5 Districts by Arrests

| Rank | District | Arrests | Observation |
|:----:|:--------:|:-------:|:------------|
| 1 | **011** | **109** | Highest arrest activity — allocate more resources here |
| 2 | **001** | **101** | Downtown/central district — high police presence |
| 3 | **025** | **80** | Third highest — needs attention |
| 4 | **008** | **74** | Significant arrest volume |
| 5 | **009** | **61** | Moderate arrest activity |

### 6.3 Key Statistics

| Metric | Value |
|:-------|------:|
| Total crime records processed | 10,000 |
| Total arrests found | **1,220** |
| **Arrest rate** | **12.2%** |
| Distinct districts | **22** |
| District with **most** arrests | **011** (109 arrests) |
| District with **fewest** arrests | **017** (26 arrests) |

### 6.4 Answer to the Commissioner

> **District 011** has the most arrests (109 out of 1,220 total) in this sample, followed closely by District 001 (101 arrests). Resources should be allocated primarily to these two districts. District 017 has the fewest arrests (26), which may indicate either lower crime rates or lower police presence in that area.

### 6.5 Missing Districts

Districts **013**, **021**, and **023** had **zero** arrests in this 10,000-record sample. They either don't exist as active districts or had no arrests in this data subset.

---

## 7. Full Execution Log

Below is the complete terminal output from the MapReduce job execution. This is what you should capture for your project reports.

```
test_student@master-node:~$ source /etc/profile.d/hadoop.sh

test_student@master-node:~$ mapred streaming \
    -files lab02_mapper_district.py,lab02_reducer_sum.py \
    -mapper "python3 lab02_mapper_district.py" \
    -reducer "python3 lab02_reducer_sum.py" \
    -input /data/chicago_crimes_sample.csv \
    -output /user/test_student/lab02/district_arrests

packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob7589528031609165454.jar tmpDir=null
2026-02-13 18:33:44,940 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-13 18:33:45,286 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-02-13 18:33:45,743 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/test_student/.staging/job_1770991083092_0013
2026-02-13 18:33:46,547 INFO mapred.FileInputFormat: Total input files to process : 1
2026-02-13 18:33:46,696 INFO mapreduce.JobSubmitter: number of splits:2
2026-02-13 18:33:47,089 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1770991083092_0013
2026-02-13 18:33:47,090 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-13 18:33:47,409 INFO conf.Configuration: resource-types.xml not found
2026-02-13 18:33:47,410 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-13 18:33:47,546 INFO impl.YarnClientImpl: Submitted application application_1770991083092_0013
2026-02-13 18:33:47,636 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1770991083092_0013/
2026-02-13 18:33:47,638 INFO mapreduce.Job: Running job: job_1770991083092_0013
2026-02-13 18:34:15,592 INFO mapreduce.Job: Job job_1770991083092_0013 running in uber mode : false
2026-02-13 18:34:15,593 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-13 18:34:47,960 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-13 18:35:05,311 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-13 18:35:06,375 INFO mapreduce.Job: Job job_1770991083092_0013 completed successfully
2026-02-13 18:35:06,848 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=9766
                FILE: Number of bytes written=963037
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2391502
                HDFS: Number of bytes written=156
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=114804
                Total time spent by all reduces in occupied slots (ms)=30000
                Total time spent by all map tasks (ms)=57402
                Total time spent by all reduce tasks (ms)=15000
                Total vcore-milliseconds taken by all map tasks=57402
                Total vcore-milliseconds taken by all reduce tasks=15000
                Total megabyte-milliseconds taken by all map tasks=29389824
                Total megabyte-milliseconds taken by all reduce tasks=7680000
        Map-Reduce Framework
                Map input records=10001
                Map output records=1220
                Map output bytes=7320
                Map output materialized bytes=9772
                Input split bytes=212
                Combine input records=0
                Combine output records=0
                Reduce input groups=22
                Reduce shuffle bytes=9772
                Reduce input records=1220
                Reduce output records=22
                Spilled Records=2440
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=1450
                CPU time spent (ms)=5100
                Physical memory (bytes) snapshot=648224768
                Virtual memory (bytes) snapshot=6557839360
                Total committed heap usage (bytes)=347807744
                Peak Map Physical memory (bytes)=253718528
                Peak Map Virtual memory (bytes)=2183729152
                Peak Reduce Physical memory (bytes)=145813504
                Peak Reduce Virtual memory (bytes)=2190712832
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=2391290
        File Output Format Counters
                Bytes Written=156
2026-02-13 18:35:06,849 INFO streaming.StreamJob: Output directory: /user/test_student/lab02/district_arrests
```

### Understanding the Counters

| Counter | Value | What It Means |
|:--------|------:|:--------------|
| Map input records | 10,001 | 10,000 data rows + 1 header row |
| Map output records | **1,220** | Only 1,220 rows had `Arrest = true` |
| Reduce input groups | **22** | 22 distinct districts found |
| Reduce output records | **22** | One output line per district |
| Launched map tasks | 2 | Hadoop split the input into 2 chunks |
| Launched reduce tasks | 1 | One reducer aggregated all results |
| CPU time spent | 5,100 ms | Total CPU across all tasks |
| HDFS Bytes Read | 2,391,502 | ~2.4 MB (the sample file) |
| Failed Shuffles | 0 | No errors during data transfer |

---

## 8. Preparing for Milestone 1

This lab is your **foundation** for the project. Here's how each Milestone 1 task maps to what you just learned:

### How to Adapt This Lab for Each Task

| Project Task | What Changes vs. This Lab | Column Index | Filter? |
|:-------------|:--------------------------|:------------:|:-------:|
| **Task 2**: Crime Type | Change key to `Primary Type` | **5** | No filter (count ALL crimes) |
| **Task 3**: Location | Change key to `Location Description` | **7** | No filter (count ALL crimes) |
| **Task 4**: Year | Parse year from `Date` field | **2** | No filter (count ALL crimes) |
| **Task 5**: Arrest | Change key to `Arrest` status | **8** | No filter (count true AND false) |

### Example: Adapting for Task 2 (Crime Type)

**This lab's mapper** extracts District and filters for arrests:

```python
# This lab (District + Arrest filter)
arrest_status = parts[8].lower()
district = parts[11]
if arrest_status == 'true':
    print(f"{district}\t1")
```

**Task 2 mapper** extracts Primary Type with NO filter:

```python
# Task 2 (Crime Type, no filter)
crime_type = parts[5].strip()
if crime_type and parts[0] != 'ID':  # Skip header
    print(f"{crime_type}\t1")
```

**The reducer stays exactly the same!** (`reducer_sum.py` works for all counting tasks.)

### Example: Adapting for Task 4 (Year — the tricky one)

The date format is: `09/05/2015 01:30:00 PM`

```python
# Task 4: Extract year from date string
date_str = parts[2].strip()          # "09/05/2015 01:30:00 PM"
date_part = date_str.split(' ')[0]   # "09/05/2015"
year = date_part.split('/')[2]       # "2015"
print(f"{year}\t1")
```

> **Hint**: You don't need `datetime` — simple `split()` operations are enough!

### Workflow Checklist for Milestone 1

```
□ Step 1: Write your mapper script (adapt from this lab)
□ Step 2: Test locally:  cat sample.csv | python3 mapper.py | sort | python3 reducer_sum.py
□ Step 3: Upload to cluster:  scp mapper.py reducer_sum.py your_id@134.209.172.50:~/
□ Step 4: SSH in + source Hadoop:  source /etc/profile.d/hadoop.sh
□ Step 5: Run on SAMPLE first:  -input /data/chicago_crimes_sample.csv
□ Step 6: Check results:  hdfs dfs -cat /user/your_id/project/m1/taskX/part-00000
□ Step 7: If correct, run on FULL dataset:  -input /data/chicago_crimes.csv
□ Step 8: Copy results + logs for your README report
```

---

## 9. Troubleshooting Guide

### Problem 1: `bash: mapred: command not found`

**Cause**: You forgot to source the Hadoop environment.

**Fix**:
```bash
source /etc/profile.d/hadoop.sh
```

> **Pro Tip**: Run this the moment you SSH in. Every. Single. Time.

---

### Problem 2: `Output directory already exists`

**Error**:
```
org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory
/user/your_id/lab02/district_arrests already exists
```

**Cause**: MapReduce refuses to overwrite existing output.

**Fix**: Delete the output directory, then re-run:
```bash
hdfs dfs -rm -r /user/your_id/lab02/district_arrests
```

---

### Problem 3: Job stuck at `map 0% reduce 0%`

**Possible Causes**:
1. Another group's job is using all cluster resources
2. Your old stuck jobs are blocking the queue

**Diagnosis**:
```bash
# Check what's running
yarn application -list
```

**Fix**: Kill your own stuck jobs:
```bash
yarn application -kill application_XXXXXXXXXX_XXXX
```

> **Be patient**: The cluster has limited memory (2 GB per worker). Only a few jobs can run simultaneously. Wait for other groups' jobs to finish.

---

### Problem 4: `Permission denied` on job submission

**Cause**: Your HDFS home directory may not exist.

**Check**:
```bash
hdfs dfs -ls /user/your_username
```

**If it doesn't exist**, ask the instructor to create it.

---

### Problem 5: Incorrect results / too many or too few lines

**Common causes**:
1. **Wrong column index** — double-check the schema (Arrest=8, District=11)
2. **Forgot to skip header** — the header row `ID,Case Number,...` will produce junk
3. **Not filtering properly** — make sure you compare against `'true'` (lowercase)

**Debug approach**: Test locally first!
```bash
head -20 sample.csv | python3 mapper_district.py
```

---

### Problem 6: SSH connection issues

If you can't SSH in:
1. Check your username is correct
2. Ensure you're on the university network (or VPN)
3. If password contains `!`, use single quotes: `'MyP@ss!word'`
4. If still failing, ask the instructor

---

## Quick Reference Card

Copy this and keep it handy:

```bash
# === EVERY SESSION ===
ssh your_id@134.209.172.50
source /etc/profile.d/hadoop.sh

# === TEST LOCALLY ===
cat test.csv | python3 mapper.py | sort | python3 reducer.py

# === UPLOAD SCRIPTS ===
scp mapper.py reducer.py your_id@134.209.172.50:~/

# === RUN JOB ===
mapred streaming \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /data/chicago_crimes_sample.csv \
    -output /user/your_id/lab02/district_arrests

# === VIEW RESULTS ===
hdfs dfs -cat /user/your_id/lab02/district_arrests/part-00000

# === RE-RUN (delete output first) ===
hdfs dfs -rm -r /user/your_id/lab02/district_arrests

# === CHECK RUNNING JOBS ===
yarn application -list

# === KILL STUCK JOB ===
yarn application -kill <application_id>
```

---

*Lab executed and verified on the class Hadoop cluster on February 13, 2026.*  
*Prepared by: Dr. Anis Koubaa — SE446 Big Data Engineering*
