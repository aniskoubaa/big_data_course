# Lab: YARN Monitoring + Advanced HiveQL (NYC Taxi)
## SE446 Big Data Engineering — Week 6

### Objective
Part A: Observe how YARN allocates resources during a live Hive query execution.  
Part B: Practice advanced HiveQL (window functions, ROLLUP, multi-table JOIN) on the NYC Taxi dataset.

### Prerequisites
- Hadoop cluster running (`ssh master@134.209.172.50`)
- HDFS path `/data/nyc_taxi/nyc_taxi_2024_sample.csv` available
- YARN Web UI accessible at `http://134.209.172.50:8088`

---

## Part A: YARN Monitoring (30 min)

### Step 1: Open YARN Web UI
Open `http://134.209.172.50:8088` in your browser. Note the current state (no running applications).

### Step 2: Submit a Long-Running Hive Query
In a terminal, start Beeline and run a query that takes a few seconds:

```bash
beeline -u 'jdbc:hive2://' \
  -e "SELECT primary_type, district, COUNT(*), AVG(CAST(arrest AS INT))
      FROM crimes GROUP BY primary_type, district ORDER BY COUNT(*) DESC;"
```

### Step 3: Observe YARN While the Query Runs
Immediately switch to the YARN Web UI. Complete this table:

| Metric | Value |
|---|---|
| Application ID | |
| Application Name | |
| Application Type | |
| AM Host | |
| State | |
| Containers Running | |
| Memory Used (MB) | |
| vCores Used | |
| Elapsed Time | |

### Step 4: After Completion — Retrieve Logs
```bash
yarn logs -applicationId <your-application-id> 2>/dev/null | head -50
```

Record:
- What framework ran the job? (look for "MRAppMaster" or "TezAppMaster")
- How many container log sections appear?

### Step 5: YARN CLI Commands
```bash
# List all applications (including finished ones)
yarn application -list -appStates ALL

# Check cluster node status
yarn node -list -all

# Check queue
yarn queue -status default
```

Record from `yarn node -list`:
- Number of nodes:
- Available memory per node (MB):
- Available vCores per node:

---

## Part B: NYC Taxi HiveQL (45 min)

### Step 1: Create the Database and Table
```sql
CREATE DATABASE IF NOT EXISTS taxi_db;
USE taxi_db;

CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi (
    tpep_pickup_datetime  STRING,
    tpep_dropoff_datetime STRING,
    passenger_count       INT,
    trip_distance         DOUBLE,
    pickup_location_id    INT,
    dropoff_location_id   INT,
    payment_type          INT,
    fare_amount           DOUBLE,
    tip_amount            DOUBLE,
    total_amount          DOUBLE,
    rate_code_id          INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/nyc_taxi/'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT COUNT(*) FROM nyc_taxi;
```

**Record total row count:**

---

### Exercise 1: Top 5 Most Expensive Trips
Write a query to find the 5 trips with the highest `total_amount`.

**Your query:**
```sql
-- Write here
```

**Results table:**

| total_amount | trip_distance | payment_type |
|---|---|---|
| | | |

---

### Exercise 2: Average Tip by Payment Type
Find the average `tip_amount` for each `payment_type`, ordered highest to lowest.

**Your query:**
```sql
-- Write here
```

**Record:** Which payment type has the highest average tip?

---

### Exercise 3: RANK Window Function
Rank all trips by `total_amount` within each `payment_type` (highest fare = rank 1).  
Show only rows where `rank <= 3`.

**Your query:**
```sql
-- Hint: use RANK() OVER (PARTITION BY payment_type ORDER BY total_amount DESC)
```

---

### Exercise 4: Running Total of Fares
Compute a running total of `fare_amount` ordered by `tpep_pickup_datetime`.  
Show the first 10 rows.

**Your query:**
```sql
-- Hint: SUM(fare_amount) OVER (ORDER BY tpep_pickup_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

---

### Exercise 5: ROLLUP — Trip Counts with Subtotals
Show trip counts grouped by `payment_type` AND `rate_code_id`, with subtotals per `payment_type` and a grand total.

**Your query:**
```sql
-- Hint: GROUP BY payment_type, rate_code_id WITH ROLLUP
```

**Record:** What does a row with `payment_type = NULL` and `rate_code_id = NULL` represent?

---

### Exercise 6: Multi-Table JOIN
Create the payment_types lookup table and join it with nyc_taxi to show the payment description instead of the integer code. Show average fare per payment description.

```sql
CREATE TABLE IF NOT EXISTS payment_types (id INT, description STRING);
INSERT INTO payment_types VALUES
    (1, 'Credit Card'), (2, 'Cash'), (3, 'No Charge'),
    (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided');
```

**Your query:**
```sql
-- Write the JOIN query here
```

---

### Exercise 7 (Extension): EXPLAIN
Run `EXPLAIN` on your Exercise 5 query. Answer:

- How many Map-Reduce stages does ROLLUP require?
- Is there a Group By operator in both Map and Reduce phases?

```sql
EXPLAIN <your exercise 5 query>;
```

---

## Submission
Submit your completed lab report (filled table + queries + results) as a PDF or `.hql` file via Moodle.
