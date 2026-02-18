# Hive Lab Execution Sequence
## SE446 Week 04 - HiveQL on Cluster

This document shows the **exact expected output** for each command in the Hive lab. Follow these steps in order.

---

## Prerequisites

### 1. SSH to Cluster
```bash
ssh user_email@134.209.172.50
# Password: (your individual password)
```

### 2. Load Hadoop Environment
```bash
source /etc/profile.d/hadoop.sh
```
*No output expected*

---

## Step 1: Verify Data Files on HDFS

### List Chicago Crime Datasets
```bash
hdfs dfs -ls /data/chicago_crimes*.csv
```

**Expected Output:**
```
-rwxr-xr-x   2 hadoop supergroup 2007709341 2026-02-13 12:30 /data/chicago_crimes.csv
-rwxr-xr-x   2 hadoop supergroup    2387194 2026-02-13 12:30 /data/chicago_crimes_sample.csv
```

### Preview Sample Data (First 3 Lines)
```bash
hdfs dfs -cat /data/chicago_crimes_sample.csv | head -3
```

**Expected Output:**
```
ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location
13311263,JG503434,07/29/2022 03:39:00 AM,023XX S TROY ST,1582,OFFENSE INVOLVING CHILDREN,CHILD PORNOGRAPHY,RESIDENCE,true,false,1033,010,25,30,17,,,2022,04/18/2024 03:40:59 PM,,,
13053066,JG103252,01/03/2023 04:44:00 PM,039XX W WASHINGTON BLVD,2017,NARCOTICS,MANUFACTURE / DELIVER - CRACK,SIDEWALK,true,false,1122,011,28,26,18,,,2023,01/20/2024 03:41:12 PM,,,
cat: Unable to write to output stream.
```

---

## Step 2: Connect to Hive with Beeline

### Start Beeline (Embedded Mode)
```bash
beeline -u 'jdbc:hive2://'
```

**Expected Output:**
```
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.4.1/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.18.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-3.4.1/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html/for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Connecting to jdbc:hive2://
Hive Session ID = [random-session-id]
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.hadoop.hive.common.StringInternUtils (file:/opt/hive/lib/hive-common-4.0.1.jar) to field java.net.URI.string
WARNING: Please consider reporting this to the maintainers of org.apache.hadoop.hive.common.StringInternUtils
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
26/02/18 [time]: WARN hikari.HikariConfig: objectstore - leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it.
26/02/18 [time]: WARN hikari.HikariConfig: objectstore-secondary - leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it.
26/02/18 [time]: WARN DataNucleus.MetaData: Metadata has jdbc-type of null yet this is not valid. Ignored
[... several similar DataNucleus warnings ...]
26/02/18 [time]: WARN metastore.ObjectStore: Version information not found in metastore. metastore.schema.verification is not enabled so recording the schema version 4.0.0
26/02/18 [time]: WARN metastore.ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 4.0.0, comment = Set by MetaStore [username]@[hostname]
26/02/18 [time]: WARN exec.FunctionRegistry: UDF Class org.apache.hive.org.apache.datasketches.hive.cpc.UnionSketchUDF does not have description. Please annotate the class with the org.apache.hadoop.hive.ql.exec.Description annotation and provide the description of the function.
[... several similar UDF description warnings ...]
26/02/18 [time]: WARN session.SessionState: Configuration hive.reloadable.aux.jars.path not specified
Connected to: Apache Hive (version 4.0.1)
Driver: Hive JDBC (version 4.0.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 4.0.1 by Apache Hive
0: jdbc:hive2://>
```

**Important:** You should see the `0: jdbc:hive2://>` prompt. This means you're connected.

---

## Step 3: Explore Hive Metastore

### Show All Databases
```sql
SHOW DATABASES;
```

**Expected Output:**
```
+----------------+
| database_name  |
+----------------+
| default        |
| test_anis_db   |
+----------------+
2 rows selected (3.818 seconds)
```

### Show Tables in Default Database
```sql
SHOW TABLES;
```

**Expected Output:**
```
+-----------------+
|    tab_name     |
+-----------------+
| chicago_crimes  |
+-----------------+
1 row selected (1.091 seconds)
```

### Describe Table Structure
```sql
DESCRIBE chicago_crimes;
```

**Expected Output:**
```
+-----------------------+------------+----------+
|       col_name        | data_type  | comment  |
+-----------------------+------------+----------+
| id                    | string     |          |
| case_number           | string     |          |
| crime_date            | string     |          |
| block                 | string     |          |
| iucr                  | string     |          |
| primary_type          | string     |          |
| description           | string     |          |
| location_description  | string     |          |
| arrest                | string     |          |
| domestic              | string     |          |
+-----------------------+------------+----------+
10 rows selected (0.575 seconds)
```

### Detailed Table Information
```sql
DESCRIBE FORMATTED chicago_crimes;
```

**Expected Output (abbreviated):**
```
+-------------------------------+----------------------------------------------------+-----------------------+
|           col_name            |                     data_type                      |        comment        |
+-------------------------------+----------------------------------------------------+-----------------------+
| id                            | string                                             |                       |
| case_number                   | string                                             |                       |
| crime_date                    | string                                             |                       |
| block                         | string                                             |                       |
| iucr                          | string                                             |                       |
| primary_type                  | string                                             |                       |
| description                   | string                                             |                       |
| location_description          | string                                             |                       |
| arrest                        | string                                             |                       |
| domestic                      | string                                             |                       |
|                               | NULL                                               | NULL                  |
| # Detailed Table Information  | NULL                                               | NULL                  |
| Database:                     | default                                            | NULL                  |
| OwnerType:                    | USER                                               | NULL                  |
| Owner:                        | [your_username]                                    | NULL                  |
| CreateTime:                   | [timestamp]                                        | NULL                  |
| LastAccessTime:               | UNKNOWN                                            | NULL                  |
| Retention:                    | 0                                                  | NULL                  |
| Location:                     | hdfs://master-node:9000/data                       | NULL                  |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                  |
|                               | EXTERNAL                                           | TRUE                  |
|                               | bucketing_version                                  | 2                     |
|                               | numFiles                                           | 2                     |
|                               | skip.header.line.count                             | 1                     |
|                               | totalSize                                          | 2010096535            |
|                               | transient_lastDdlTime                              | [timestamp]           |
|                               | NULL                                               | NULL                  |
| # Storage Information         | NULL                                               | NULL                  |
| SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe | NULL                  |
| InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat           | NULL                  |
| OutputFormat:                 | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat | NULL                  |
| Compressed:                   | No                                                 | NULL                  |
| Num Buckets:                  | -1                                                 | NULL                  |
| Bucket Columns:               | []                                                 | NULL                  |
| Sort Columns:                 | []                                                 | NULL                  |
| Storage Desc Params:          | NULL                                               | NULL                  |
|                               | field.delim                                        | ,                     |
|                               | serialization.format                               | ,                     |
+-------------------------------+----------------------------------------------------+-----------------------+
39 rows selected (0.708 seconds)
```

---

## Step 4: Run HiveQL Queries

### Query 1: Top 10 Crime Types
```sql
SELECT primary_type, COUNT(*) AS cnt
FROM chicago_crimes
GROUP BY primary_type
ORDER BY cnt DESC
LIMIT 10;
```

**Expected MapReduce Progress:**
```
26/02/18 [time]: WARN mapreduce.Counters: Group org.apache.hadoop.mapred.Task$Counter is deprecated. Use org.apache.hadoop.mapreduce.TaskCounter instead
2026-02-18 [time],[ms] Stage-2 map = 0%,  reduce = 0%
2026-02-18 [time],[ms] Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 3.66 sec
2026-02-18 [time],[ms] Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 7.41 sec
MapReduce Total cumulative CPU time: 7 seconds 410 msec
Ended Job = job_[random-job-id]
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 8  Reduce: 8   Cumulative CPU: 99.71 sec   HDFS Read: 2010284294 HDFS Write: 2012 HDFS EC Read: 0 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 7.41 sec   HDFS Read: 12790 HDFS Write: 397 HDFS EC Read: 0 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 47 seconds 120 msec
```

**Expected Query Results:**
```
+----------------------+----------+
|     primary_type     |   cnt    |
+----------------------+----------+
| THEFT                | 1806117  |
| BATTERY              | 1548929  |
| CRIMINAL DAMAGE      | 966789   |
| NARCOTICS            | 765949   |
| ASSAULT              | 571260   |
| OTHER OFFENSE        | 530725   |
| BURGLARY             | 448990   |
| MOTOR VEHICLE THEFT  | 437209   |
| DECEPTIVE PRACTICE   | 393628   |
| ROBBERY              | 316579   |
+----------------------+----------+
10 rows selected (302.715 seconds)
```

---

## Step 5: Using HQL Script Files (Professional Practice)

### Exit Beeline
```sql
!quit
```

**Expected Output:**
```
Closing: 0: jdbc:hive2://
```

### Create HQL Script
```bash
nano top_crimes.hql
```

**Paste this content into nano:**
```sql
-- Top 10 Crime Types Query
-- Make sure you are using your database
USE default;

SELECT primary_type, COUNT(*) AS cnt
FROM chicago_crimes
GROUP BY primary_type
ORDER BY cnt DESC
LIMIT 10;
```

**Save and exit nano:**
- Press `CTRL+O` (Save)
- Press `Enter` (Confirm filename)
- Press `CTRL+X` (Exit)

### Execute HQL Script (Method 1: Command Line)
```bash
beeline -u 'jdbc:hive2://' -f top_crimes.hql
```

**Expected Output:** Same results as Step 4, but without interactive Beeline session.

### Execute HQL Script (Method 2: Inside Beeline)
```bash
beeline -u 'jdbc:hive2://'
```
Then at the prompt:
```sql
SOURCE top_crimes.hql;
```

**Expected Output:** Same results as Step 4.

### Execute Single Query (Method 3: Inline)
```bash
beeline -u 'jdbc:hive2://' -e "SELECT COUNT(*) FROM chicago_crimes;"
```

**Expected Output:**
```
+--------+
|  _c0   |
+--------+
| 7789766|
+--------+
1 row selected ([time] seconds)
```

---

## Step 6: Exit and Cleanup

### Exit Beeline
```sql
!quit
```

### Verify Script File
```bash
cat top_crimes.hql
```

**Expected Output:**
```
-- Top 10 Crime Types Query
-- Make sure you are using your database
USE default;

SELECT primary_type, COUNT(*) AS cnt
FROM chicago_crimes
GROUP BY primary_type
ORDER BY cnt DESC
LIMIT 10;
```

---

## Common Issues and Solutions

### Issue 1: "No current connection" Error
**Cause:** Used `hive` command instead of `beeline -u 'jdbc:hive2://'`
**Solution:** Always use `beeline -u 'jdbc:hive2://'` to connect immediately

### Issue 2: "Permission denied" on HDFS
**Cause:** Query results cache permission conflicts between users
**Solution:** This has been fixed system-wide by disabling the cache

### Issue 3: "Table not found" Error
**Cause:** Table created in different database than where you're querying
**Solution:** Use `USE default;` or specify database: `default.chicago_crimes`

### Issue 4: Derby metastore lock error
**Cause:** Previous Beeline session didn't close properly
**Solution:** 
```bash
pkill -u $USER -f beeline
rm -f $HOME/hive_metastore_db/db.lck $HOME/hive_metastore_db/dbex.lck
```

---

## Performance Notes

- **First query** on a large dataset takes ~5 minutes (MapReduce job compilation)
- **Subsequent queries** are faster (~2-3 minutes)
- **Sample dataset** (`chicago_crimes_sample.csv`) runs in seconds
- **Full dataset** (`chicago_crimes.csv`) shows the power of distributed processing

---

## Quick Reference Commands

| Task | Command |
|------|---------|
| Connect to Hive | `beeline -u 'jdbc:hive2://'` |
| Run HQL file | `beeline -u 'jdbc:hive2://' -f script.hql` |
| Run single query | `beeline -u 'jdbc:hive2://' -e "SELECT..."` |
| Exit Beeline | `!quit` |
| Show databases | `SHOW DATABASES;` |
| Show tables | `SHOW TABLES;` |
| Describe table | `DESCRIBE table_name;` |
| Describe formatted | `DESCRIBE FORMATTED table_name;` |

---

## Success Criteria

✅ Connected to Hive without errors  
✅ Listed databases and tables  
✅ Described table structure  
✅ Ran aggregation query with MapReduce  
✅ Created and executed HQL script file  
✅ Understood the difference between interactive and script execution  

---

**Note:** All timestamps, session IDs, and job IDs in this document are examples and will vary in your actual execution. The format and structure of the output should match exactly.
