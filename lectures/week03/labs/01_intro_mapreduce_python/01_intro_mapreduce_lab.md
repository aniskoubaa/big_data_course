# Lab 01: Introduction to MapReduce with Python & Hadoop Streaming

**Course**: SE446 - Big Data Engineering

**Objective**: Learn how to write MapReduce jobs in Python using the **Hadoop Streaming** interface and run them on a real Hadoop cluster.

---

## 🏗️ What is Hadoop Streaming?

Hadoop is written in Java, but you can write MapReduce jobs in **Python** using the "Hadoop Streaming" utility.

- **How it works**: Hadoop treats your code as executable scripts.
- **Data Flow**:
  1.  **Mapper**: Reads data from **Standard Input (stdin)**, writes to **Standard Output (stdout)**.
  2.  **Shuffle/Sort**: Hadoop handles this automatically (sorting keys).
  3.  **Reducer**: Reads sorted pairs from **stdin**, writes final results to **stdout**.

**Rule of Thumb**: If it prints to the terminal, it works in Hadoop!

---

## 🛠️ Step 1: The Mapper (`mapper.py`)

The Mapper reads text line-by-line, breaks it into words, and outputs `(word, 1)`.

### 1.1 Create the file `mapper.py`

Create a new file named `mapper.py`. This script acts as the **Map** function.

**Crucial Code Explained:**

- `sys.stdin`: Think of this as an **input pipe**. The script doesn't open a specific file (like "data.txt"). Instead, it blindly reads whatever text is "fed" to it from the outside world. This is why we can feed it data using `cat` locally or HDFS on the cluster.

* `print(f"{word}\t1")`: We output a **Key-Value Pair**. The `\t` (Tab character) is the separator Hadoop expects by default.

**Pseudocode:**

```text
FUNCTION map(input_line):
    words = SPLIT input_line
    FOR EACH word IN words:
        EMIT (word, 1)
```

```python
#!/usr/bin/env python3
import sys

# Loop through each line of input from Standard Input
for line in sys.stdin:
    # 1. Cleaning: Remove leading/trailing whitespace (like newline characters)
    line = line.strip()

    # 2. Splitting: Break line into words (default splits on spaces)
    words = line.split()

    # 3. Emitting: Output (mapped) data
    for word in words:
        # Format: Key \t Value
        # We output '1' for every occurrence. The Reducer will sum these later.
        print(f"{word}\t1")
```

**Mapper Input/Output Visualization:**

| Input (One line) | Output (Stdout)          |
| :--------------- | :----------------------- |
| `hello world`    | `hello  1`<br>`world  1` |

### 1.2 Test on Linux Terminal (Crucial Step!)

**"Locally"** here means running standard Python on the **Linux file system**, completely bypassing HDFS. This simulates the MapReduce logic using Linux pipes.

**Recommendation**: Connect to the **Master Node** via SSH/Terminal _now_, and create your files there.

> **Reminder: Connection Command**
>
> ```bash
> ssh <your_username>@134.209.172.50
> ```
>
> _Use the password sent to your email._

- **Why?** If you create `mapper.py` on the Master Node, it is already ready usage in Step 4.
- **How?** Use `nano mapper.py` or VS Code Remote to create the file on the server.

1.  Create a dummy input file `test.txt` **on the Linux file system**:
    ```text
    hello world
    hello hadoop
    ```
2.  Run the mapper using the pipe `|` operator:

    ```bash
    cat test.txt | python3 mapper.py
    ```

    **Expected Output:**

    ```text
    hello   1
    world   1
    hello   1
    hadoop  1
    ```

---

## 🛠️ Step 2: The Reducer (`reducer.py`)

The Reducer receives the output from the Mapper.

**IMPORTANT**: Hadoop guarantees that the input to the Reducer is **sorted by key**.

- **Without Sorting**: `hello 1`, `world 1`, `hello 1`
- **With Sorting**: `hello 1`, `hello 1`, `world 1` -> This allows us to just check the "next" line to see if we are still counting the same word.

### 2.1 Create the file `reducer.py`

Create a new file named `reducer.py`.

**Crucial Logic Explained:**

- **Sorted Input**: We assume identical keys arrive together (e.g., `apple`, `apple`, `banana`).
- **State Machine**: We track `current_word`. If the new word is the same, we increment `current_count`. If it's different, we print the total and start counting the new word.

**Pseudocode:**

```text
FUNCTION reduce(key, list_of_values):
    current_key = NULL
    current_sum = 0

    FOR EACH input_key, value IN sorted_input:
        IF input_key MATCHES current_key:
            current_sum = current_sum + value
        ELSE:
            EMIT (current_key, current_sum)
            current_key = input_key
            current_sum = value

    EMIT (current_key, current_sum) // Don't forget the last one!
```

```python
#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Input comes from STDIN (which is the output of the Mapper + Sort)
for line in sys.stdin:
    # Remove whitespace
    line = line.strip()

    # Parse the input: we expect "key \t value"
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue # Skip bad lines

    # LOGIC: Check if this word is the same as the one we are currently counting
    if current_word == word:
        current_count += count
    else:
        # We encountered a NEW word.
        # 1. Print the result for the PREVIOUS word (if it exists)
        if current_word:
            print(f"{current_word}\t{current_count}")
        # 2. Reset the counters for the NEW word
        current_word = word
        current_count = count

# Don't forget to print the last word!
if current_word == word:
    print(f"{current_word}\t{current_count}")
```

**Reducer Input/Output Visualization:**

| Input (Sorted)                            | Output (Aggregated)        |
| :---------------------------------------- | :------------------------- |
| `apple   1`<br>`apple   1`<br>`banana  1` | `apple   2`<br>`banana  1` |

### 2.2 Test Logic Locally

Test the reducer by simulating sorted input:

```bash
# Echo sends text, 'sort' sorts it, then reducer processes it
echo -e "foo\t1\nfoo\t1\nbar\t1" | sort | python3 reducer.py
```

**Expected Output:**

```text
bar     1
foo     2
```

---

## 🔗 Step 3: The Full Pipeline (Detailed Tracing)

This command simulates the entire MapReduce process on the **Linux Command Line**. It verifies that your Mapper and Reducer work together correctly before you burden the cluster.

```bash
cat test.txt | python3 mapper.py | sort | python3 reducer.py
```

### 🧐 Visualization of Data Flow

Let's trace exactly what happens to the data at each stage of the pipe:

**1. Input (`cat test.txt`)**

> Raw text file.

```text
hello world
hello hadoop
```

**2. Map Phase (`| python3 mapper.py`)**

> Breaks lines into words. Emits (Key, 1). Note that 'hello' appears twice and is NOT grouped yet.

```text
hello   1
world   1
hello   1
hadoop  1
```

**3. Shuffle/Sort Phase (`| sort`)**

> **Critical Step!** Groups identical keys together. Now 'hello' lines are adjacent.

```text
hadoop  1
hello   1  <-- Adjacent
hello   1  <-- Adjacent
world   1
```

**4. Reduce Phase (`| python3 reducer.py`)**

> Iterates through the sorted list, summing the 1s for each group.

```text
hadoop  1
hello   2
world   1
```

If you see this result, your logic is perfect!

---

## 🚀 Step 4: Running on the Hadoop Cluster

Now that your code works on the Linux command line, let's submit it to the **HDFS Cluster**.

### 4.1 Input Data to HDFS

1.  Ensure you are connected to the Master Node.
2.  Put your `test.txt` into **HDFS** (Distributed Storage).
    - _Note_: `test.txt` moves from your local Linux folder -> HDFS.
    ```bash
    hdfs dfs -mkdir -p /user/your_id/lab01
    hdfs dfs -put test.txt /user/your_id/lab01/
    ```

### 4.2 Run the MapReduce Job

Run the following command. This submits your `mapper.py` and `reducer.py` (which are on the Linux filesystem) to the Hadoop Scheduler.

```bash
mapred streaming \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /user/your_id/lab01/test.txt \
    -output /user/your_id/lab01/output
```

- `-files`: Uploads your python scripts to every node in the cluster.
- `-mapper`: The command to run the mapper.
- `-reducer`: The command to run the reducer.
- `-input`: Where data lives in HDFS.
- `-output`: Where to write results **(Must NOT exist!)**.

---

## 📊 Step 5: View Results

Once the job finishes (Success!), check the output directory.

```bash
# List output files
hdfs dfs -ls /user/your_id/lab01/output

# View the result content
hdfs dfs -cat /user/your_id/lab01/output/part-00000
```

---

## 🏆 Project Milestone 1 Preparation

For your **Milestone 1 (Chicago Crime Project)**, you will follow this exact pattern:

1.  **Mapper**: Instead of splitting words, you will split CSV lines (e.g., `line.split(',')`).
2.  **Key Selection**: You will select the 'Primary Type' or 'Year' column as your key.
3.  **Reducer**: Sums will work exactly the same way.

**Tip**: Always handle CSV parsing carefully (watch out for commas inside quotes!).
