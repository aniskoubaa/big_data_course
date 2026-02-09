# Project Milestone 1: Chicago Crime Analytics with MapReduce

**Due Date**: [Insert Date]  
**Group Size**: 3-5 Students  
**Submission**: GitHub Repository Link via LMS

## 1. Scenario: The Digital Detectives

You and your team have been hired as **Junior Data Engineers** for the **Chicago Police Department (CPD)**. The department sits on terabytes of crime reports dating back to 2001, but they lack the tools to analyze it efficiently.

Your Commander needs actionable intelligence—fast. He wants to know **what** crimes are happening, **where** they are concentrated, and **how** crime patterns change over time.

Your mission is to build a **MapReduce pipeline** on the department's Hadoop Cluster to answer these critical questions.

---

## 2. Prerequisites & Resources

- ✅ **Completed Lab 02**: You should have completed [Lab 02: Intermediate MapReduce](../lectures/week03/labs/02_intermediate_mapreduce_python/02_intermediate_mapreduce_lab.md).
  - _Why?_ Lab 02 provides the base code for parsing the Chicago Crime CSV (`mapper_district.py`) and summing counts (`reducer_sum.py`). You will adapt this code for this project.
- **Cluster Access**: Follow the [Student Instructions](../administration/student_access/STUDENT_INSTRUCTIONS.md).

---

## 3. Dataset

The dataset is shared on the HDFS cluster at:

- **Full Dataset**: `/data/chicago_crimes.csv` (Use this for your final run)
- **Sample**: `/data/chicago_crimes_sample.csv` (Use this for testing)

**Schema**: `ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrest, ...`

---

## 4. Required Intel (Tasks)

### Task 1: GitHub Setup

1.  Create a **Private** GitHub repository named `se446-project-group-X`.
2.  Add `akoubaa` (Instructor) and TAs as collaborators.
3.  Structure: `/src` (Code), `/scripts` (Shell scripts), `/output` (Results).

### Task 2: Crime Type Distribution

- **Mission**: The Commander wants a breakdown of all crime categories.
- **Action**: Write a MapReduce job to count occurrences of each `Primary Type`.
- **Input Column**: Index 5 (`Primary Type`).

### Task 3: Location Hotspots

- **Mission**: Patrol units need to know where to deploy.
- **Action**: Write a MapReduce job to count crimes by `Location Description`.
- **Input Column**: Index 7 (`Location Description`).

### Task 4: The Time Dimension (Challenge)

- **Mission**: Is crime increasing or decreasing?
- **Action**: Parse the `Date` column (e.g., `09/05/2015 01:30:00 PM`) to extract the **Year** and count crimes per year.
- **Input Column**: Index 2 (`Date`). _Hint: You need Python's string splitting or datetime module._

---

## 5. Deployment & Reporting

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
    - **Execution Logs**: Copy-paste a snippet of the terminal output showing `Map 100% Reduce 100%` (evidence of success).
4.  **Member Contribution**: A table summarizing what each member did (e.g., "Ali wrote the mapper", "Sara handled the scripts").

---

## 6. Evaluation Criteria

### Group Evaluation (60%)

- **Correctness**: Jobs run successfully and produce correct counts.
- **Code Quality**: Clean, commented Python code.
- **Repository Structure**: Professional organization.
- **Report**: Clarity of instructions and insights in `README.md`.

### Individual Evaluation (40%)

- **Task Ownership**: Each student must be the **primary author** of at least one substantial component (e.g., a Mapper script, the execution shell script, or the Report Analysis).
  - _Requirement_: The "Member Contribution" table in your README must clearly state who owned which file/task.
- **GitHub Evidence**: We will verify that the user who "owned" a task actually pushed the code for it.
- **In-Class Validation**: Random team members will be asked to explain parts of the code. **If you cannot explain the code you supposedly wrote, you will receive a zero.**

## 7. AI Usage Policy

We encourage the **responsible** use of AI tools (ChatGPT, GitHub Copilot).

- ✅ **Allowed**:
  - Using AI to **debug** errors (e.g., "Why does this line fail?").
  - Asking for **conceptual explanations** (e.g., "Explain how the reducer gets sorted keys").
  - Generating **comments** or documentation.
- ❌ **Prohibited**:
  - Asking AI to "Write the MapReduce job for me" from scratch.
  - Copy-pasting entire files without understanding them.
- **Penalty**: If you submit code you validly cannot explain during the in-class check, you will receive a **Zero**, regardless of whether AI wrote it or your teammate wrote it.
