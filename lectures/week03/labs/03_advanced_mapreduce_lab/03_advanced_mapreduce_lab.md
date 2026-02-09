# Lab 03: Advanced MapReduce Concepts (Python Simulation)

**Course:** SE446 - Big Data Engineering  
**Lab:** Week 3 - Advanced Concepts  
**Duration:** 60 minutes  
**Prerequisites:**

- Completed **Lab 01** (Intro)
- Completed **Lab 02** (Cluster Execution)

---

## 📋 Lab Objectives

Now that you have run real jobs on the cluster (Lab 02), we will step back to purely **Python-based simulation** to understand complex algorithms that are hard to debug on a cluster.

By the end of this lab, you will be able to:

1.  ✅ **Simulate** the Shuffle/Sort phase locally in Python.
2.  ✅ Implement **Advanced Filtering** (finding arrests).
3.  ✅ Calculate **Derived Metrics** (Arrest Rates %).
4.  ✅ Chain **Multi-Stage Jobs** (e.g., finding the "Top 5" items).

> **Note**: This lab runs entirely in a **Jupyter Notebook** or local Python script. It focuses on logic correctness before you deploy to the massive cluster.

---

## 🔧 Setup

### Part 0: Environment Setup

You can complete this lab in:

- **Google Colab** (recommended)
- **Local Jupyter Notebook**
- **VS Code**

```python
# Run this cell first to set up the MapReduce framework
from collections import defaultdict
import pandas as pd

def map_reduce(data, mapper, reducer):
    """
    Simple MapReduce implementation in Python.

    Parameters:
    - data: List of input records
    - mapper: Function (record) -> (key, value) or list of (key, value) or None
    - reducer: Function (key, [values]) -> (key, result)

    Returns:
    - List of (key, result) tuples
    """
    # MAP PHASE
    mapped = []
    for record in data:
        result = mapper(record)
        if result is not None:
            if isinstance(result, list):
                mapped.extend(result)
            else:
                mapped.append(result)

    # SHUFFLE PHASE
    shuffled = defaultdict(list)
    for key, value in mapped:
        shuffled[key].append(value)

    # REDUCE PHASE
    results = []
    for key, values in shuffled.items():
        result = reducer(key, values)
        if result is not None:
            results.append(result)

    return results

print("✅ MapReduce framework ready!")
```

---

## 📊 Part 1: Word Count - The Hello World of MapReduce

**Objective**: Understand the basic MapReduce pattern with word count.

### Step 1.1: Basic Word Count

```python
# Sample text data
documents = [
    "Hello World Hello",
    "World of Big Data",
    "Big Data is the Future",
    "Hello Future World"
]

# Mapper: for each word, emit (word, 1)
def word_mapper(line):
    """
    Input: A line of text
    Output: List of (word, 1) tuples
    """
    words = line.lower().split()
    return [(word, 1) for word in words]

# Reducer: sum all counts for each word
def count_reducer(word, counts):
    """
    Input: word, list of counts [1, 1, 1, ...]
    Output: (word, total)
    """
    return (word, sum(counts))

# Run MapReduce
word_counts = map_reduce(documents, word_mapper, count_reducer)

# Display results
print("Word Counts:")
for word, count in sorted(word_counts, key=lambda x: x[1], reverse=True):
    print(f"  {word}: {count}")
```

### 📝 Question 1.1

What is the count for the word "world"? Why?

**Your Answer:** ******\_\_\_******

### Step 1.2: Trace the Data Flow

For the input `["A B A", "B C"]`, manually trace each phase:

| Phase        | Input     | Output |
| ------------ | --------- | ------ |
| Map (line 1) | "A B A"   | ?      |
| Map (line 2) | "B C"     | ?      |
| Shuffle      | all pairs | ?      |
| Reduce       | grouped   | ?      |

```python
# Verify your answer
test_data = ["A B A", "B C"]
test_result = map_reduce(test_data, word_mapper, count_reducer)
print("Result:", dict(test_result))
```

---

## 🔍 Part 2: Crime Data Analysis

### Step 2.1: Load the Dataset

```python
# Load Chicago crime data
url = "https://raw.githubusercontent.com/alfaisal-se446/data/main/chicago_crimes_sample.csv"

# Alternative if URL doesn't work:
# Create sample data
import pandas as pd

try:
    crimes_df = pd.read_csv(url)
except:
    # Fallback sample data
    crimes_df = pd.DataFrame({
        'ID': range(1, 1001),
        'Primary Type': ['THEFT']*300 + ['BATTERY']*250 + ['ASSAULT']*150 +
                       ['CRIMINAL DAMAGE']*100 + ['BURGLARY']*100 + ['OTHER OFFENSE']*100,
        'District': [1]*100 + [2]*150 + [3]*200 + [4]*150 + [5]*100 +
                   [6]*100 + [7]*100 + [8]*100,
        'Arrest': [True]*350 + [False]*650,
        'Location Description': ['STREET']*400 + ['RESIDENCE']*300 +
                               ['APARTMENT']*150 + ['RETAIL STORE']*150
    })

# Convert to list of dictionaries
crime_records = crimes_df.to_dict('records')

print(f"✅ Loaded {len(crime_records):,} crime records")
print(f"📋 Columns: {list(crimes_df.columns)}")
crimes_df.head()
```

### Step 2.2: Count Crimes by Type

```python
# TODO: Implement the mapper
def crime_type_mapper(record):
    """
    Input: One crime record (dict)
    Output: (crime_type, 1)

    Hint: Access the 'Primary Type' field from the record
    """
    # YOUR CODE HERE
    crime_type = record['Primary Type']
    return (crime_type, 1)

# Run MapReduce (reuse count_reducer from Part 1)
crime_counts = map_reduce(crime_records, crime_type_mapper, count_reducer)

# Display top 10
print("\n📊 Top 10 Crime Types:")
print("-" * 40)
for crime_type, count in sorted(crime_counts, key=lambda x: x[1], reverse=True)[:10]:
    print(f"{crime_type:25} {count:>6,}")
```

### 📝 Question 2.2

What is the most common crime type in the dataset?

**Your Answer:** ******\_\_\_******

---

## 🏢 Part 3: Crimes by District

### Step 3.1: Count Crimes per District

```python
# TODO: Implement the mapper
def district_mapper(record):
    """
    Input: One crime record (dict)
    Output: (district, 1)
    """
    # YOUR CODE HERE
    pass  # Replace with your code

# Run MapReduce
district_counts = map_reduce(crime_records, district_mapper, count_reducer)

# Display results
print("\n🏢 Crimes by District:")
print("-" * 30)
for district, count in sorted(district_counts, key=lambda x: x[1], reverse=True):
    print(f"District {district:>3}: {count:>6,}")
```

### 📝 Question 3.1

Which district has the most crimes?

**Your Answer:** ******\_\_\_******

---

## 🚔 Part 4: Filtering with MapReduce

### Step 4.1: Only Crimes with Arrests

**Pattern**: Return `None` from mapper to filter out records.

```python
def arrest_filter_mapper(record):
    """
    Only emit crimes where an arrest was made.

    Input: Crime record
    Output: (crime_type, 1) if arrest, else None
    """
    if record.get('Arrest') == True:
        return (record['Primary Type'], 1)
    else:
        return None  # This record is filtered out

# Run MapReduce
arrest_counts = map_reduce(crime_records, arrest_filter_mapper, count_reducer)

print("\n🚔 Crimes with Arrests:")
print("-" * 40)
for crime_type, count in sorted(arrest_counts, key=lambda x: x[1], reverse=True)[:10]:
    print(f"{crime_type:25} {count:>6,}")
```

### 📝 Question 4.1

For which crime type were the most arrests made?

**Your Answer:** ******\_\_\_******

---

## 📈 Part 5: Calculating Arrest Rate

### Step 5.1: Compute Arrest Rate per Crime Type

**Challenge**: The value must contain both arrest count AND total count.

```python
def arrest_rate_mapper(record):
    """
    Emit (crime_type, (arrested, total))

    - arrested: 1 if arrest made, 0 otherwise
    - total: always 1
    """
    crime_type = record['Primary Type']
    arrested = 1 if record.get('Arrest') == True else 0
    return (crime_type, (arrested, 1))

def arrest_rate_reducer(crime_type, values):
    """
    Calculate arrest rate.

    Input: list of (arrested, total) tuples
    Output: (crime_type, {'arrests': n, 'total': m, 'rate': %})
    """
    total_arrests = sum(v[0] for v in values)
    total_crimes = sum(v[1] for v in values)

    rate = (total_arrests / total_crimes * 100) if total_crimes > 0 else 0

    return (crime_type, {
        'arrests': total_arrests,
        'total': total_crimes,
        'rate': round(rate, 1)
    })

# Run MapReduce
arrest_rates = map_reduce(crime_records, arrest_rate_mapper, arrest_rate_reducer)

# Display results
print("\n📈 Arrest Rates by Crime Type:")
print("-" * 55)
print(f"{'Crime Type':25} {'Arrests':>8} {'Total':>8} {'Rate':>8}")
print("-" * 55)
for crime_type, stats in sorted(arrest_rates, key=lambda x: x[1]['rate'], reverse=True)[:10]:
    print(f"{crime_type:25} {stats['arrests']:>8,} {stats['total']:>8,} {stats['rate']:>7.1f}%")
```

### 📝 Question 5.1

Which crime type has the highest arrest rate? What is the rate?

**Your Answer:** ******\_\_\_******

### 📝 Question 5.2

Why might some crime types have higher arrest rates than others?

**Your Answer:** ******\_\_\_******

---

## 🏆 Part 6: Multi-Stage MapReduce - Top 5

### Step 6.1: Find Top 5 Crime Types

**Approach**: Two MapReduce jobs

1. Count crimes by type
2. Find top 5 from the counts

```python
# Stage 1: Count by type (already have crime_counts)
print("Stage 1: Counting crimes by type...")
crime_counts = map_reduce(crime_records, crime_type_mapper, count_reducer)
print(f"  → {len(crime_counts)} crime types")

# Stage 2: Find top 5
def top_mapper(item):
    """Send all to one reducer using dummy key"""
    crime_type, count = item
    return ("ALL", (crime_type, count))

def top_5_reducer(key, values):
    """Sort and take top 5"""
    sorted_values = sorted(values, key=lambda x: x[1], reverse=True)
    return (key, sorted_values[:5])

print("\nStage 2: Finding top 5...")
top_5 = map_reduce(crime_counts, top_mapper, top_5_reducer)

# Display results
print("\n🏆 Top 5 Crime Types:")
print("-" * 40)
for rank, (crime_type, count) in enumerate(top_5[0][1], 1):
    print(f"#{rank}: {crime_type:25} {count:>6,}")
```

### 📝 Question 6.1

Why do we need TWO MapReduce stages for finding top 5?

**Your Answer:** ******\_\_\_******

---

## 🧪 Part 7: Challenge Exercises

### Challenge 7.1: Crime Locations by Type

Find the top 3 locations for each crime type.

```python
# TODO: Implement multi-stage MapReduce

# Stage 1: Count (crime_type, location) combinations
def type_location_mapper(record):
    """
    Output: ((crime_type, location), 1)
    """
    # YOUR CODE HERE
    pass

# Stage 2: Group by crime type, find top 3 locations
def group_mapper(item):
    """
    Input: ((crime_type, location), count)
    Output: (crime_type, (location, count))
    """
    # YOUR CODE HERE
    pass

def top_3_reducer(crime_type, values):
    """
    Sort locations by count, return top 3
    """
    # YOUR CODE HERE
    pass

# Run both stages
# stage1_result = map_reduce(crime_records, type_location_mapper, count_reducer)
# final_result = map_reduce(stage1_result, group_mapper, top_3_reducer)
```

### Challenge 7.2: Time-Based Analysis

If your dataset has a 'Date' column, analyze crimes by hour of day.

```python
# TODO: Implement if Date column exists
# Hint: Parse date, extract hour, use as key
```

---

## ✅ Lab Completion Checklist

| Task                                  | Status |
| ------------------------------------- | ------ |
| Implemented word count mapper/reducer | ⬜     |
| Traced MapReduce data flow manually   | ⬜     |
| Counted crimes by type                | ⬜     |
| Counted crimes by district            | ⬜     |
| Filtered crimes with arrests          | ⬜     |
| Calculated arrest rates               | ⬜     |
| Implemented multi-stage top 5         | ⬜     |
| Answered all questions                | ⬜     |

---

## 📝 Lab Submission

### Required Deliverables

1. **Completed Notebook**
   - All code cells executed
   - All questions answered
2. **GitHub Commit**
   - Push to your team's repository
   - Folder: `milestone_2_mapreduce/student_<yourname>/`
   - Commit message: `"M2: Completed Lab 02 MapReduce exercises"`

3. **ExamGPT Submission**
   - Answer in-class quiz questions
   - Be prepared to explain YOUR code

---

## 🔍 Troubleshooting

### Common Errors

**1. TypeError: 'NoneType' object is not iterable**

```python
# Problem: Mapper returns None but framework tries to iterate
# Solution: Check if result is None before processing

# In map_reduce function:
if result is not None:
    mapped.append(result)
```

**2. KeyError: 'Primary Type'**

```python
# Problem: Column name mismatch
# Solution: Check actual column names
print(crime_records[0].keys())
```

**3. Wrong counts**

```python
# Problem: Mapper returns wrong type
# Solution: Ensure mapper returns (key, value) tuple, not just value
return (crime_type, 1)  # Correct
return crime_type  # Wrong!
```

---

## 📚 Resources

- **Slides**: `../slides/SE446_W03B_mapreduce_python.pdf`
- **Notebook**: `../notebooks/SE446_W03B_mapreduce_practice.ipynb`
- **Textbook**: Chapter 3 - MapReduce Fundamentals
- **Google Paper**: "MapReduce: Simplified Data Processing on Large Clusters"

---

**Lab 02 Complete!** 🎉

Next: Week 4 - Milestone 2 Work Session & MapReduce on Hadoop Cluster
