# Project Milestone 1: Chicago Crime Analytics with MapReduce

**Due Date**: Saturday, February 21, 2024

**Group Size**: 3-5 Students

**Submission**: Instructions will be announced later.

## 1. Scenario: The Digital Detectives

You and your team have been hired as **Junior Data Engineers** for the **Chicago Police Department (CPD)**. The department sits on terabytes of crime reports dating back to 2001, but they lack the tools to analyze it efficiently.

Your Commander needs actionable intelligence—fast. He wants to know **what** crimes are happening, **where** they are concentrated, and **how** crime patterns change over time.

Your mission is to build a **MapReduce pipeline** on the department's Hadoop Cluster to answer these critical questions.

---

## 2. Prerequisites & Resources

- ✅ **Completed Lab 02**: You should have completed [Lab 02: Intermediate MapReduce](../lectures/week03/labs/02_intermediate_mapreduce_python/02_intermediate_mapreduce_lab.md).
  - _Why?_ Lab 02 provides the base code for parsing the Chicago Crime CSV (`mapper_district.py`) and summing counts (`reducer_sum.py`). You will adapt this code for this project.
  - 📖 **Lab Execution Report**: Review [Lab 02 Execution Report](../lectures/week03/labs/02_intermediate_mapreduce_python/02_intermediate_mapreduce_lab_execution_report.md) for:
    - Complete worked example with real cluster execution
    - Dataset schema reference (column indices for all fields)
    - Step-by-step execution workflow
    - **Section 8: "Preparing for Milestone 1"** — shows exactly how to adapt the lab code for each project task
    - Full troubleshooting guide with solutions to common errors
- **Cluster Access**: This is private info (IP, Key). **Do not commit generated keys or IPs to GitHub**. You received the access instructions via email.

---

## 3. Dataset

The dataset is shared on the HDFS cluster at:

- **Full Dataset**: `/data/chicago_crimes.csv` (Use this for your final run)
- **Sample**: `/data/chicago_crimes_sample.csv` (Use this for testing)

**Schema**: `ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrest, ...`

---

## 4. Required Tasks

### Task 1: GitHub Setup

1.  Create a **Private** GitHub repository named `se446-project-group-X`.
2.  Add `akoubaa` (Instructor) and TAs as collaborators.
3.  Structure: `/src` (Code), `/scripts` (Shell scripts), `/output` (Results).
4.  **README**: You must have a `README.md` file that contains the **Group Name** and the **Names of all Students**.
5.  **Registration**: Each group must be registered here: [Group Registration Form](https://docs.google.com/forms/d/e/1FAIpQLSf5gabWze14TkXbXfOsArZgFvaEPXV1vtyeBsbb2SVL-qoAsA/viewform).

### Task 2: Crime Type Distribution

- **Research Question**: What are the most common types of crimes in Chicago?
- **Mission**: The Commander wants a statistical breakdown of all crime categories to allocate resources effectively.
- **Action**: Write a MapReduce job to count the total occurrences of each crime type (e.g., THEFT, BATTERY, NARCOTICS).
- **Technical Detail**: The crime type is found in the **`Primary Type`** column, which is at **index 5** (0-based index) of the CSV line.

### Task 3: Location Hotspots

- **Research Question**: Where do most crimes occur?
- **Mission**: Patrol units need to know which locations (e.g., STREET, SIDEWALK, APARTMENT) are high-risk zones.
- **Action**: Write a MapReduce job to count the total occurrences of crimes for each location type.
- **Technical Detail**: The location description is found in the **`Location Description`** column, which is at **index 7** (0-based index) of the CSV line.

### Task 4: The Time Dimension (Challenge)

- **Research Question**: How has the total number of crimes changed over the years?
- **Mission**: Is crime increasing or decreasing? We need to visualize the trend of crime volume per year.
- **Action**: Parse the date string (e.g., `09/05/2015 01:30:00 PM`) to extract the **Year** and count the total number of crimes for each year.
- **Technical Detail**: The date is found in the **`Date`** column, which is at **index 2** (0-based index) of the CSV line.
  - _Hint_: You will need to use Python's `split()` method or the `datetime` module to isolate the year from the full date string.

---

### Task 5: Law Enforcement Analysis

- **Research Question**: What percentage of crimes result in an arrest?
- **Mission**: The Police Chief needs to assess the efficiency of patrols. How often are criminals actually caught?
- **Action**: Write a MapReduce job to count the number of **True** (Arrested) vs. **False** (Not Arrested) values.
- **Technical Detail**: The arrest status is found in the **`Arrest`** column, which is at **index 8** (0-based index).

---

### 📚 Task Adaptation Guide

Unsure how to start? The [Lab 02 Execution Report — Section 8](../lectures/week03/labs/02_intermediate_mapreduce_python/02_intermediate_mapreduce_lab_execution_report.md#8-preparing-for-milestone-1) shows:

- **Exact code changes** needed for each task (from District → Crime Type → Location → Year → Arrest)
- **Column index reference table** for quick lookup
- **Date parsing example** for Task 4 (the tricky one)
- Which tasks need filtering vs. counting all records

The reducer (`reducer_sum.py` from Lab 02) works for **all tasks** without modification.

---

## Quick Start: Running MapReduce on the Cluster

> **💡 First Time?** Read the [Lab 02 Execution Report](../lectures/week03/labs/02_intermediate_mapreduce_python/02_intermediate_mapreduce_lab_execution_report.md) first — it shows a complete end-to-end example with the same dataset and explains every step in detail.

This is all you need to run a mapper + reducer on the Hadoop cluster.

### Step 1: Write Your Scripts Locally

Create two Python files on your laptop:
- `mapper.py` — reads from `sys.stdin`, prints `key\tvalue`
- `reducer.py` — reads sorted `key\tvalue` from `sys.stdin`, sums per key

### Step 2: Test Locally Before Uploading

```bash
# Download a small sample to your laptop first
head -100 some_data.csv > test.csv

# Simulate the full MapReduce pipeline locally:
cat test.csv | python3 mapper.py | sort | python3 reducer.py
```

If this prints correct output, your scripts work.

### Step 3: Upload Scripts to the Cluster

```bash
scp mapper.py reducer.py your_id@134.209.172.50:~/
```

### Step 4: SSH into the Cluster and Run

```bash
ssh your_id@134.209.172.50
source /etc/profile.d/hadoop.sh

mapred streaming \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /data/chicago_crimes_sample.csv \
  -output /user/your_id/project/m1/task2
```

### Step 5: View Results

```bash
hdfs dfs -cat /user/your_id/project/m1/task2/part-00000
```

### Step 6: Re-run? Delete Output First

MapReduce refuses to overwrite. Delete the output before each re-run:

```bash
hdfs dfs -rm -r /user/your_id/project/m1/task2
```

---

## Hints & Common Pitfalls

> Read these **before** you start. They will save you hours.

**1. Always test on the sample first.**
Use `/data/chicago_crimes_sample.csv` (10K rows). Only switch to the full dataset (`/data/chicago_crimes.csv`, 8M+ rows) for your final run.

**2. Source the Hadoop environment every time you log in.**
```bash
source /etc/profile.d/hadoop.sh
```
Without this, `mapred`, `hdfs`, and `yarn` commands will not be found.

**3. "Output directory already exists" error.**
MapReduce will not overwrite. You must delete the output directory before re-running:
```bash
hdfs dfs -rm -r /user/your_id/project/m1/taskX
```

**4. "Permission denied" on job submit.**
If you see a permission error when submitting a job, make sure your HDFS home directory exists:
```bash
hdfs dfs -ls /user/your_id
```
If it does not exist, ask the instructor.

**5. Job stuck at 0% or "ACCEPTED" forever.**
The cluster has limited memory. If another group's job is running, yours waits in queue. Be patient. Check with:
```bash
yarn application -list
```
If you see your own old stuck jobs, kill them:
```bash
yarn application -kill <application_id>
```

**6. Task 4 hint — extracting the year from a date.**
The date looks like `09/05/2015 01:30:00 PM`. You don't need `datetime`. Think about `split()`: first split by space to get the date part, then split by `/` to get the year.

**7. Skip the CSV header line.**
The first line of the CSV is a header (`ID,Case Number,Date,...`). Your mapper must skip it. A simple check: if the first field equals `ID`, skip that line.

**8. Password tip.**
If your password contains `!`, bash treats it as a special character. Either enclose the password in single quotes or escape it with `\!`.

---

## 5. Work Distribution & Git Workflow (CRITICAL)

To ensure fair grading, you must follow this strict workflow.

### 🟥 The "One Student, One Task" Rule

- **Requirement**: Each group member must be responsible for **at least one task** (Task 2, 3, 4, or 5).
- **Evidence**: You must write the code AND commit it to GitHub yourself.

### 🤖 Automated Grading

We use an automated system to analyze the repository history.

- It scans the **commit logs**.
- If you claim to have done "Task 2" in the report, but the commits belong to "UserX", **you will receive a ZERO**.
- **Rule**: Your GitHub username must appear in the commit history for your assigned task.

### 🔄 The Pull Request Workflow

Do not push everything directly to the `main` branch. Simulate a real software engineering environment:

1.  **Branching**: The student working on a task creates a new branch (e.g., `git checkout -b task2-ali`).
2.  **Committing**: The student commits their code to this branch.
3.  **Pull Request (PR)**: The student opens a **Pull Request** on GitHub to merge their branch into `main`.
4.  **Group Leader**: The Group Leader reviews the PR and clicks **Merge**.

---

## 6. Deployment & Reporting

### Step 1: Execute on Cluster

Run your jobs on the Hadoop Cluster using `mapred streaming`.

- **Input**: `/data/chicago_crimes.csv`
- **Output**: `/user/your_id/project/m1/taskX/`

### Step 2: Project Report (`README.md`)

Your GitHub `README.md` is your official report to the Commander. It must include:

1.  **Team Members**: List Names and IDs.
2.  **Executive Summary**: A brief paragraph implementation logic.
3.  **For EACH Task (2, 3, and 4)**:
    - **Instructions**: The exact `mapred streaming` command you used.
    - **Sample Results**: Paste the **Top 5 lines** of your output (e.g., `THEFT 1000`).
    - **Interpretation**: One sentence interpreting the result (e.g., _"Theft is the predominant crime, accounting for 20% of all incidents."_).
    - **Execution Logs**: You must copy-paste the **FULL terminal output** of the MapReduce job execution (from the initial command to the final success message). This allows us to verify the job ran successfully on the cluster.
4.  **Member Contribution**: A table summarizing what each member did (e.g., "Ali wrote the mapper", "Sara handled the scripts").

---

## 7. Evaluation Criteria

Your score will be automatically computed based on:

1.  **Code Output (50%)**: Do your MapReduce jobs produce the correct numbers?
2.  **Report Completeness (20%)**: Does your `README.md` contain all sections, names, and logs?
3.  **Git Contribution (30%)**: Did _every_ member commit code? If you have 0 commits, your individual grade is 0.

_Note: We reserve the right to manually audit any submission and ask questions in class._

## 8. AI Usage Policy

We encourage the **responsible** use of AI tools (ChatGPT, GitHub Copilot).

- ✅ **Allowed**:
  - Using AI to **debug** errors (e.g., "Why does this line fail?").
  - Asking for **conceptual explanations** (e.g., "Explain how the reducer gets sorted keys").
  - Generating **comments** or documentation.
- ❌ **Prohibited**:
  - Asking AI to "Write the MapReduce job for me" from scratch.
  - Copy-pasting entire files without understanding them.
- **Penalty**: If you submit code you validly cannot explain during the in-class check, you will receive a **Zero**, regardless of whether AI wrote it or your teammate wrote it.
