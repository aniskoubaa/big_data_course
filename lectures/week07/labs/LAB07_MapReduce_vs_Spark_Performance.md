# SE 446 — Big Data Engineering
## Lab 07: MapReduce vs Apache Spark — A Performance Comparison

| | |
|---|---|
| **Course** | SE 446 — Big Data Engineering |
| **Lab** | 07 |
| **Topic** | MapReduce vs Spark: Speed, Caching, and Iterative Processing |
| **Estimated Time** | 90 minutes |
| **Cluster** | master-node: `134.209.172.50` |

---

## What You Will Learn

By the end of this lab you will be able to:

1. Connect to a real Hadoop/Spark cluster using your personal credentials
2. Run a Word Count job using **Hadoop MapReduce** three consecutive times and measure performance
3. Run the same Word Count using **Apache Spark** with in-memory caching and measure performance
4. Compare the two frameworks and explain **why Spark is faster for iterative workloads**

---

## Before You Start — What You Need

- Your **personal login credentials** (username and password) provided by the instructor
- A **terminal** application on your laptop:
  - macOS / Linux: use the built-in Terminal app
  - Windows: use **Git Bash** or **Windows Subsystem for Linux (WSL)**
- **Git** installed on your laptop (`git --version` to verify)
- Basic familiarity with the Linux command line (navigating folders, running scripts)

> **Note:** You do NOT need to install Hadoop or Spark on your laptop. Everything runs on the cluster. Your laptop is only used to connect and issue commands.

---

## Step 0 — Clone the Lab Repository to the Cluster

### Why clone to the cluster?

The lab scripts live on GitHub. You will **clone the repository directly onto the cluster** so the scripts are available where Hadoop and Spark are running.

### 0.1 — Connect to the cluster

Open your terminal and type the following command. Replace `studentXX` with your own username (e.g., `student01`, `student02`, ...):

```bash
ssh studentXX@134.209.172.50
```

> **What is SSH?** SSH (Secure Shell) is a protocol that lets you log in to a remote computer securely over the internet. After running this command, you are working *inside* the cluster — not on your own laptop.

You will be prompted for a password. Type the password provided by your instructor (characters will not appear on screen — this is normal).

You should see a welcome prompt like:

```
Welcome to Ubuntu 20.04 LTS
studentXX@master-node:~$
```

The `$` means you are now logged in and ready to type commands on the cluster.

---

### 0.2 — Clone the repository

Now that you are on the cluster, clone the course GitHub repository:

```bash
git clone https://github.com/aniskoubaa/big_data_course
```

> **What is `git clone`?** This downloads a complete copy of the repository from GitHub to the cluster. Think of it like downloading a zip file, but smarter — it tracks all changes and can be updated later.

You will see output like:

```
Cloning into 'big_data_course'...
remote: Enumerating objects: ...
Receiving objects: 100% ...
Done.
```

---

### 0.3 — Navigate to the lab folder

Move into the lab directory:

```bash
cd big_data_course/lectures/week07/labs
```

> **What is `cd`?** It stands for *change directory* — the same as double-clicking a folder in a file explorer.

To confirm you are in the right place, list the contents:

```bash
ls -l
```

You should see the `scripts/` folder and this lab document listed.

---

## Step 1 — Prepare the Dataset

### What are we doing?

Before running any jobs, we need data to process. This step generates a large text file (~150 MB) full of words and uploads it to **HDFS** — the Hadoop Distributed File System — where both MapReduce and Spark can access it.

> **What is HDFS?** HDFS is a special file system designed to store very large files across multiple machines. It is NOT the same as the regular Linux file system. Files stored in HDFS are split into blocks and distributed across the cluster's hard drives. When you run a MapReduce or Spark job, the data is read from HDFS automatically.

### 1.1 — Generate and upload the dataset

Run this Python script. Replace `studentXX` with your actual username:

```bash
python3 scripts/01_setup_data.py --student studentXX
```

> **What does this script do?**
> 1. It creates a large text file (~150 MB) in `/tmp/lab07_corpus.txt` on the master-node
> 2. It creates a directory in HDFS at `/user/studentXX/lab07/`
> 3. It uploads the text file to HDFS so Hadoop and Spark can read it

This will take 1–2 minutes. You will see progress output like:

```
Generating corpus...  150 MB
Uploading to HDFS: /se446/lab07/studentXX/corpus.txt
Done! Dataset is ready.
```

### 1.2 — Verify the upload

```bash
hdfs dfs -ls /user/studentXX/lab07/
```

> **What is `hdfs dfs -ls`?** This is like running `ls` but inside HDFS instead of the regular Linux file system. You should see `corpus.txt` listed with its size (~157 MB).

Expected output:

```
Found 1 items
-rw-r--r--  3  studentXX  hadoop  157286400  ...  /user/studentXX/lab07/corpus.txt
```

If you see this, you are ready to continue.

---

## Step 2 — Run MapReduce (3 Jobs)

### Why MapReduce and why 3 times?

MapReduce is the original Hadoop processing model. For this experiment, we will run the **same Word Count job three consecutive times**. This simulates a realistic scenario where an analyst re-runs the same query multiple times.

> **Key insight:** Every MapReduce job reads the entire dataset from HDFS from scratch. Even if you run the same job twice, it re-reads all the data both times. This is one of MapReduce's main limitations for iterative workloads.

### 2.1 — Run the 3 MapReduce jobs

Replace `studentXX` with your username:

```bash
bash scripts/02_run_mapreduce.sh /user/studentXX/lab07
```

> **What is `bash script.sh`?** This tells the Linux shell to run all the commands written inside the script file. You can read it yourself with `cat scripts/02_run_mapreduce.sh`.

This script will:
1. Run **MapReduce Job 1** — reads corpus from HDFS, computes word counts, writes results to HDFS
2. Run **MapReduce Job 2** — repeats the exact same job (reads all data from HDFS again)
3. Run **MapReduce Job 3** — repeats again (reads all data from HDFS again)
4. Save all timing results to `/tmp/mr_benchmark.log`

Each job takes **3–6 minutes**. You will see output ending with:

```
=====================================
MAPREDUCE JOB 1 TIME: XX.XX seconds
MAPREDUCE JOB 2 TIME: XX.XX seconds
MAPREDUCE JOB 3 TIME: XX.XX seconds
=====================================
MAPREDUCE TOTAL TIME: XX.XX seconds
=====================================
```

> **Why is it slow?** Each job launches a new Java Virtual Machine, negotiates resources from YARN (the cluster resource manager), reads all 150 MB from disk, shuffles data across the network, and writes results back to disk. This entire process repeats three times.

### 2.2 — Record your MapReduce times

| Job | Time (seconds) |
|-----|---------------|
| MapReduce Job 1 | _______ |
| MapReduce Job 2 | _______ |
| MapReduce Job 3 | _______ |
| **Total** | **_______** |

---

## Step 3 — Run Apache Spark

### Why Spark is different

Spark takes a fundamentally different approach:

1. **First action:** Spark reads the data from HDFS once and keeps it in **RAM (executor memory)**. This is called **caching** or **persisting** the RDD.
2. **Subsequent actions:** Spark reads directly from RAM — no disk access, no HDFS I/O.

> **What is an RDD?** RDD stands for *Resilient Distributed Dataset* — Spark's core data structure. Think of it as a very large list split across all the machines in the cluster and held in their memory.

### 3.1 — Run the Spark job

Replace `studentXX` with your username:

```bash
spark-submit \
  --master yarn \
  --executor-memory 1g \
  --num-executors 2 \
  scripts/03_run_spark.py /user/studentXX/lab07/corpus.txt
```

> **What do these options mean?**
> - `--master yarn` — use YARN (the cluster resource manager) to distribute the Spark job
> - `--executor-memory 1g` — give each worker 1 GB of RAM to hold the cached data
> - `--num-executors 2` — use 2 worker processes across the cluster
> - The last argument is the HDFS path to your corpus file

This script will:
1. Load the corpus from HDFS and **cache it in RAM**
2. Run **Spark Action 1** — count all unique words (includes loading from disk the first time)
3. Run **Spark Action 2** — filter words appearing ≥ 100 times (reads from RAM)
4. Run **Spark Action 3** — find the top 10 most frequent words (reads from RAM)
5. Save timing results to `/tmp/spark_benchmark.log`

Output:

```
Loading data and caching RDD...
=====================================
SPARK ACTION 1 TIME: XX.XX seconds
SPARK ACTION 2 TIME: XX.XX seconds
SPARK ACTION 3 TIME: XX.XX seconds
=====================================
SPARK TOTAL TIME: XX.XX seconds
=====================================
```

> **What should you notice?** Action 1 may take similar time to a MapReduce job (it still reads from disk the first time). But Actions 2 and 3 should be **dramatically faster** — the data is already in RAM.

### 3.2 — Record your Spark times

| Action | Time (seconds) |
|--------|---------------|
| Spark Action 1 (first run — loads from disk, then caches) | _______ |
| Spark Action 2 (reads from RAM) | _______ |
| Spark Action 3 (reads from RAM) | _______ |
| **Total** | **_______** |

---

## Step 4 — Compare the Results

### 4.1 — Run the comparison script

```bash
bash scripts/04_compare.sh /tmp/mr_benchmark.log /tmp/spark_benchmark.log
```

> **What does this script do?** It reads both log files and prints a side-by-side table with timings and the overall speedup factor.

Expected output (your numbers will differ):

```
============================================================
       MapReduce vs Spark — Performance Comparison
============================================================
                       MapReduce    Spark
------------------------------------------------------------
 Run / Action 1        348.2 s      72.4 s
 Run / Action 2        351.8 s       1.3 s
 Run / Action 3        349.6 s       0.9 s
------------------------------------------------------------
 TOTAL                1049.6 s      74.6 s
 SPEEDUP                           14.1x
============================================================
 Extrapolated to 10 runs:
   MapReduce: ~3498.7 s  (~58 min)
   Spark:     ~80.0 s    (< 2 min)
============================================================
```

### 4.2 — Calculate the speedup

$$\text{Speedup} = \frac{\text{MapReduce Total (s)}}{\text{Spark Total (s)}}$$

Record your speedup: **_________ x**

---

## Step 5 — Explore the Web UIs

These browser-based dashboards let you see what happened inside the cluster visually.

### 5.1 — Spark History Server

After your Spark job finishes, open this URL in your **laptop's** web browser:

```
http://134.209.172.50:18080
```

> **What is the History Server?** It stores information about completed Spark jobs so you can review them after they finish.

Navigate to your application and explore:
- **Jobs tab** — you should see 3 jobs; notice how much shorter Jobs 2 and 3 are
- **Storage tab** — confirm the RDD is shown as `Cached` and note its size in MB

Take a **screenshot of the Storage tab** for your lab report.

### 5.2 — YARN Resource Manager

```
http://134.209.172.50:8088
```

> **What is YARN?** YARN (Yet Another Resource Negotiator) is the cluster's resource manager — it allocates CPU and memory to applications. Find your completed MapReduce jobs and Spark application here.

Compare the number of containers (tasks) used by MapReduce vs Spark.

---

## Step 6 — Discussion Questions

Answer these in your lab report:

**Q1.** How many times faster was Spark overall?  
Show your calculation using the speedup formula from Step 4.2.

**Q2.** Compare the **first run only** (MapReduce Job 1 vs Spark Action 1).  
Was Spark already significantly faster on the very first run? Explain why or why not — consider that both frameworks had to read from HDFS the first time.

**Q3.** Compare **runs/actions 2 and 3** specifically.  
How much faster were Spark Actions 2 and 3 compared to MapReduce Jobs 2 and 3?  
Explain architecturally: where is the data coming from in each case?

**Q4.** In the Spark History Server Storage tab:
- What is the cached RDD size in MB?
- What fraction of it is stored in memory vs on disk?
- Include your screenshot in the report.

**Q5.** In the YARN Resource Manager, how many containers (tasks) did the MapReduce run use compared to Spark?  
What does this difference tell you about startup overhead?

**Q6.** If you needed to run this same analysis **10 times**, estimate the total time for each framework.  
Which would you choose and why?

---

## Submission Checklist

- [ ] **MapReduce timing table** — individual times for Jobs 1, 2, 3 and the total
- [ ] **Spark timing table** — individual times for Actions 1, 2, 3 and the total
- [ ] **Speedup calculation** with formula shown (Q1)
- [ ] **Written answers** to Q2, Q3, Q5, Q6
- [ ] **Screenshot** — Spark History Server → Storage tab (Q4)
- [ ] **Screenshot** — YARN Resource Manager showing both applications (Q5)

---

## Key Concepts Summary

| | MapReduce | Apache Spark |
|---|---|---|
| **Data source each run** | HDFS (disk) — every single run | HDFS on 1st run only; RAM after |
| **Intermediate results** | Written to HDFS (disk) | Kept in RAM (executor memory) |
| **Job startup cost** | High — new JVM + YARN negotiation per job | Low — single long-running application |
| **Best suited for** | Single-pass batch ETL | Iterative workloads, repeated queries, ML |
| **API richness** | Map + Reduce only | Python, SQL, ML, Streaming, Graph |

> **Architect's rule of thumb:**  
> If your workload touches the same dataset more than once → **use Spark**.  
> For a single massive ETL pass where data does not fit in cluster RAM → MapReduce is still reasonable.

---

## Troubleshooting

| Problem | What to do |
|---------|-----------|
| `ssh: Connection refused` | Check your username spelling. Ask the instructor to verify your account. |
| `Permission denied (publickey,password)` | Ensure you are typing the password correctly. Passwords are case-sensitive. |
| `hdfs: command not found` | Run: `export PATH=/opt/hadoop/bin:$PATH` |
| `spark-submit: command not found` | Run: `export PATH=/opt/spark/bin:$PATH` |
| `git: command not found` | Run: `sudo apt-get install -y git` |
| `01_setup_data.py` fails with HDFS error | Check that your HDFS home exists: `hdfs dfs -ls /user/studentXX/` |
| `Output directory already exists` | Run: `hdfs dfs -rm -r /user/studentXX/lab07/mr_out_*` then retry |
| Spark killed by YARN (out of memory) | Change `--executor-memory 1g` to `--executor-memory 512m` and retry |
| No output after job completes | Check logs: `yarn logs -applicationId <appId>` (appId printed at job start) |
| Script not found | Verify your location: `pwd` should end with `.../big_data_course/lectures/week07/labs` |
