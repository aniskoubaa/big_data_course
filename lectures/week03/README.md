# Week 3: MapReduce Fundamentals

## Overview

This week introduces the MapReduce programming model - the foundational paradigm for distributed data processing. Students will learn how to break down complex data analysis tasks into Map and Reduce phases that can run across a cluster.

---

## Session 3A: MapReduce Concepts

### Learning Objectives
1. Understand the MapReduce programming model (Map → Shuffle → Reduce)
2. Explain how MapReduce achieves parallel processing
3. Identify when MapReduce is appropriate vs. other approaches
4. Trace data flow through a MapReduce job

### Pre-Class Video
**"MapReduce Explained"** - Computerphile (~15 min)  
🔗 https://www.youtube.com/watch?v=cvhKoniK5Uo

**Alternative**: "MapReduce Basics" - Simplilearn (~20 min)  
🔗 https://www.youtube.com/watch?v=SqvAaB3vK8U

### Materials
- 📊 Slides: `slides/SE446_W03A_mapreduce_concepts.pdf`
- 📓 Notebook: `notebooks/SE446_W03A_mapreduce_intro.ipynb`

---

## Session 3B: Implementing MapReduce in Python

### Learning Objectives
1. Implement Mapper functions in Python
2. Implement Reducer functions in Python
3. Chain Map and Reduce operations for multi-stage processing
4. Debug common MapReduce errors

### Pre-Class Video
**"Python MapReduce Tutorial"** - Derek Banas (~18 min)  
🔗 https://www.youtube.com/watch?v=nOKVh3t6x4g

### Materials
- 📊 Slides: `slides/SE446_W03B_mapreduce_python.pdf`
- 📓 Notebook: `notebooks/SE446_W03B_mapreduce_practice.ipynb`
- 🔬 Lab: `labs/02_mapreduce_lab/`

---

## Key Concepts

### MapReduce Paradigm

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MAPREDUCE DATA FLOW                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  INPUT DATA                                                                 │
│  ─────────────                                                              │
│  Block 1: "Hello World Hello"                                              │
│  Block 2: "World Big Data"                                                 │
│  Block 3: "Hello Big Data World"                                           │
│           │                                                                 │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        MAP PHASE                                    │   │
│  │  ───────────────────────────────────────────────────────────────   │   │
│  │  Mapper 1: "Hello World Hello"                                      │   │
│  │            → (Hello, 1), (World, 1), (Hello, 1)                    │   │
│  │                                                                     │   │
│  │  Mapper 2: "World Big Data"                                        │   │
│  │            → (World, 1), (Big, 1), (Data, 1)                       │   │
│  │                                                                     │   │
│  │  Mapper 3: "Hello Big Data World"                                  │   │
│  │            → (Hello, 1), (Big, 1), (Data, 1), (World, 1)           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│           │                                                                 │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     SHUFFLE & SORT                                  │   │
│  │  ───────────────────────────────────────────────────────────────   │   │
│  │  Group by key:                                                      │   │
│  │    Big   → [1, 1]                                                  │   │
│  │    Data  → [1, 1]                                                  │   │
│  │    Hello → [1, 1, 1]                                               │   │
│  │    World → [1, 1, 1]                                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│           │                                                                 │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                       REDUCE PHASE                                  │   │
│  │  ───────────────────────────────────────────────────────────────   │   │
│  │  Reducer: Sum values for each key                                  │   │
│  │    Big   → 2                                                       │   │
│  │    Data  → 2                                                       │   │
│  │    Hello → 3                                                       │   │
│  │    World → 3                                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│           │                                                                 │
│           ▼                                                                 │
│  OUTPUT: {Big: 2, Data: 2, Hello: 3, World: 3}                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Terminology

| Term | Definition |
|------|------------|
| **Mapper** | Function that processes input records and emits (key, value) pairs |
| **Reducer** | Function that aggregates values for each unique key |
| **Shuffle** | System process that groups mapper outputs by key |
| **Combiner** | Optional local reducer that runs on mapper output (optimization) |
| **Partitioner** | Determines which reducer receives which keys |

### MapReduce vs Traditional Processing

| Aspect | Traditional | MapReduce |
|--------|-------------|-----------|
| **Data Location** | Move data to computation | Move computation to data |
| **Parallelism** | Manual threading | Automatic across cluster |
| **Fault Tolerance** | Custom handling | Built-in re-execution |
| **Scalability** | Limited by single machine | Linear with cluster size |

---

## In-Class Lab: Crime Analysis with MapReduce

Students will implement MapReduce to analyze Chicago crime data:

### Lab Tasks
1. **Word Count** - Count crime types
2. **Aggregation** - Sum crimes per district
3. **Filtering** - Find crimes with arrests
4. **Multi-stage** - Top 5 crime locations

### Sample Code Pattern

```python
# Mapper: emit (crime_type, 1) for each crime
def crime_type_mapper(record):
    crime_type = record['Primary Type']
    return (crime_type, 1)

# Reducer: sum all counts for each crime type
def count_reducer(key, values):
    return (key, sum(values))
```

---

## ExamGPT Topics

The in-class quiz will cover:
- Map function input/output format
- Reduce function input/output format
- Shuffle phase purpose
- Identifying parallelization opportunities
- Debugging MapReduce logic errors

---

## Homework

Before Week 4:
1. Complete Milestone 2 starter notebook (MapReduce)
2. Implement mapper and reducer for crime analysis
3. Commit your work to GitHub with meaningful messages
4. Watch pre-class videos for Week 4

---

## Connection to HDFS (Week 2)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   HOW MAPREDUCE USES HDFS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. INPUT: Data stored in HDFS blocks across DataNodes                     │
│                                                                             │
│  2. DATA LOCALITY: Mappers run ON the DataNode containing the block        │
│     → Minimizes network transfer (computation moves to data)               │
│                                                                             │
│  3. OUTPUT: Reducer results written back to HDFS                           │
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │  DataNode 1  │     │  DataNode 2  │     │  DataNode 3  │                │
│  │  ──────────  │     │  ──────────  │     │  ──────────  │                │
│  │  Block A     │     │  Block B     │     │  Block C     │                │
│  │      ↓       │     │      ↓       │     │      ↓       │                │
│  │  Mapper 1    │     │  Mapper 2    │     │  Mapper 3    │                │
│  │  (local)     │     │  (local)     │     │  (local)     │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│          │                   │                   │                          │
│          └─────────────┬─────┴───────────────────┘                          │
│                        ▼                                                    │
│                 [ Shuffle & Sort ]                                         │
│                        ↓                                                    │
│                  [ Reducers ]                                              │
│                        ↓                                                    │
│                   HDFS Output                                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Resources

- 📖 **Textbook Chapter 3**: MapReduce Fundamentals
- 🔗 **Apache Hadoop Docs**: https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/
- 📺 **Google's Original Paper**: "MapReduce: Simplified Data Processing on Large Clusters"
