# Lab 01: HDFS Fundamentals - Hands-On Exploration

**Course:** SE446 - Big Data Engineering  
**Week:** 2  
**Duration:** 60 minutes  
**Difficulty:** Beginner

---

## 🎯 Learning Objectives

By the end of this lab, you will be able to:

1. **Connect** to a remote HDFS cluster using SSH
2. **Explore** the HDFS cluster architecture (NameNode, DataNodes)
3. **Navigate** the HDFS filesystem using basic commands
4. **Upload** files to HDFS and observe data distribution
5. **Understand** HDFS blocks, replication, and metadata

---

## 📋 Prerequisites

- ✅ You have received your cluster credentials via email
- ✅ Terminal access (Mac/Linux) or PowerShell (Windows)
- ✅ Basic knowledge of Linux commands

---

## 🚀 Part 1: Connecting to the Cluster

### Step 1.1: SSH Login

Open your terminal and connect to the cluster:

```bash
ssh <your_username>@134.209.172.50
```

**Example:**
```bash
ssh akoubaa@134.209.172.50
```

Enter your password when prompted (from your credentials email).

**✅ Success Check:** You should see a welcome message and a command prompt.

> [!CAUTION]
> **CRITICAL: Two Separate Filesystems!**
> 
> The cluster has **TWO** different filesystems. Do not confuse them!
> 
> 1. **Linux Filesystem (Local)**
>    - Path: `/home/akoubaa`
>    - Commands: `cd`, `ls`, `cat`, `pwd`
>    - This is where you are when you login via SSH.
> 
> 2. **HDFS Filesystem (Distributed)**
>    - Path: `/user/akoubaa`
>    - Commands: `hdfs dfs -ls`, `hdfs dfs -cat`, etc.
>    - **You CANNOT use `cd` into HDFS!** It is not a mountable filesystem.
>
> **Common Mistake:**
> ```bash
> # ❌ WRONG - This will fail!
> cd /user/akoubaa
> 
> # ✅ CORRECT
> hdfs dfs -ls /user/akoubaa
> ```

---

## 🔍 Part 2: Exploring Cluster Architecture

### Step 2.1: Check Cluster Status

See overall cluster health and capacity:

```bash
hdfs dfsadmin -report
```

**Sample Output:**
```
Configured Capacity: 102719209472 (95.66 GB)
Present Capacity: 90407325696 (84.20 GB)
DFS Remaining: 90407235584 (84.20 GB)
DFS Used: 90112 (88 KB)
DFS Used%: 0.00%
Replicated Blocks:
        Under replicated blocks: 0
        Blocks with corrupt replicas: 0
        Missing blocks: 0
Live datanodes (2): ...
```

**What to look for:**
- **Configured Capacity:** Total storage (~95 GB)
- **DFS Used:** How much space is occupied
- **Live datanodes:** Should show 2 worker nodes
- **Replication factor:** Default is 2

**📝 Question:** How much storage is available in the cluster?

---

### Step 2.2: View Running Java Processes

See what HDFS services are running:

```bash
jps
```

**You should see:**
- No NameNode (you're on a client machine)
- DataNode processes may appear if you're on a worker node

**Note:** Students connect as clients, not to the master node directly.

---

### Step 2.3: Check Cluster Configuration

View important cluster settings:

```bash
hdfs getconf -confKey dfs.replication
```

**Expected output:**
```
2
```
(each file is replicated 2 times)

Try these too:
```bash
# Block size (how large each block is)
hdfs getconf -confKey dfs.blocksize
```

**Sample Output:**
```
134217728
```
(134217728 bytes = 128 MB)

```bash
# NameNode address
hdfs getconf -confKey fs.defaultFS
```

**Sample Output:**
```
hdfs://0.0.0.0:9000
```

**📝 Question:** What is the default block size in bytes? Convert to MB.

---

## 📂 Part 3: HDFS Filesystem Navigation

### Step 3.1: List Your Home Directory

Check your personal HDFS directory:

```bash
hdfs dfs -ls /user/<your_username>
```

**Example:**
```bash
hdfs dfs -ls /user/akoubaa
```

**Expected:** Empty directory (or with any files you created previously).

> [!WARNING]
> Remember: Do not try `cd /user/akoubaa`. That directory does not exist in Linux! Use `hdfs dfs -ls` instead.

---

### Step 3.2: Create Directories

Practice creating directories in HDFS:

```bash
# Create a single directory
hdfs dfs -mkdir /user/<your_username>/lab01

# Create nested directories
hdfs dfs -mkdir -p /user/<your_username>/data/input
hdfs dfs -mkdir -p /user/<your_username>/data/output
```

**Verify:**
```bash
hdfs dfs -ls /user/<your_username>
hdfs dfs -ls -R /user/<your_username>
```

**💡 Tip:** `-R` means recursive (shows all subdirectories).

---

### Step 3.3: Check Directory Permissions

```bash
hdfs dfs -ls /user/<your_username>
```

**Understanding the output:**
```bash
drwxr-xr-x   - akoubaa hadoop          0 2026-02-02 10:30 /user/akoubaa/lab01
```

- `d`: Directory
- `rwxr-xr-x`: Permissions (owner can read/write/execute)
- `akoubaa`: Owner
- `hadoop`: Group
- `0`: Size
- Rest: Timestamp and path

---

## 📤 Part 4: Uploading Files to HDFS

### Step 4.1: Create a Test File

Create a small file on the local filesystem:

```bash
echo "Hello HDFS! This is my first file." > test.txt
echo "Big Data is awesome!" >> test.txt
echo "HDFS stores data in blocks." >> test.txt
```

**Verify local file:**
```bash
cat test.txt
ls -lh test.txt
```

---

### Step 4.2: Upload to HDFS

Copy the file from local to HDFS:

```bash
hdfs dfs -put test.txt /user/<your_username>/lab01/
```

**Alternative methods:**
```bash
# Same as -put
hdfs dfs -copyFromLocal test.txt /user/<your_username>/lab01/test2.txt
```

**Verify upload:**
```bash
hdfs dfs -ls /user/<your_username>/lab01/
```

---

### Step 4.3: Read File from HDFS

Display file contents:

```bash
hdfs dfs -cat /user/<your_username>/lab01/test.txt
```

**Alternative:** View first few lines only:
```bash
hdfs dfs -head /user/<your_username>/lab01/test.txt
```

---

## 🧱 Part 5: Understanding Blocks and Replication

### Step 5.1: Create a Larger File

Generate a file large enough to span multiple blocks:

```bash
# Create a ~150 MB file (larger than default 128 MB block size)
dd if=/dev/urandom of=bigfile.dat bs=1M count=150
```

**Check local file size:**
```bash
ls -lh bigfile.dat
```

---

### Step 5.2: Upload Large File

```bash
hdfs dfs -put bigfile.dat /user/<your_username>/lab01/
```

⏱️ **This will take ~10-30 seconds depending on network speed.**

---

### Step 5.3: Check File Status

View detailed information about the file:

```bash
hdfs fsck /user/<your_username>/lab01/bigfile.dat -files -blocks -locations
```

**What you'll see:**
- **Total size:** ~150 MB
- **Number of blocks:** 2 (one 128 MB block + one smaller block)
- **Block locations:** Which DataNodes store each block
- **Replication:** Each block appears on 2 DataNodes

**📝 Exercise:** 
1. How many blocks does your file have?
2. On which DataNodes are the blocks stored?
3. How many replicas exist in total?

---

### Step 5.4: View File Statistics

Get quick file info:

```bash
hdfs dfs -stat "Size: %b bytes, Replication: %r, Block Size: %o" /user/<your_username>/lab01/bigfile.dat
```

---

## 🔄 Part 6: File Operations

### Step 6.1: Copy Files Within HDFS

```bash
# Copy file to another location in HDFS
hdfs dfs -cp /user/<your_username>/lab01/test.txt /user/<your_username>/data/input/
```

---

### Step 6.2: Move/Rename Files

```bash
# Rename a file
hdfs dfs -mv /user/<your_username>/lab01/test2.txt /user/<your_username>/lab01/renamed.txt
```

---

### Step 6.3: Download from HDFS

Copy file from HDFS back to local:

```bash
hdfs dfs -get /user/<your_username>/lab01/test.txt downloaded_test.txt
```

**Verify:**
```bash
cat downloaded_test.txt
```

---

### Step 6.4: Delete Files

⚠️ **Be careful!** HDFS delete is permanent (no recycle bin).

```bash
# Delete a single file
hdfs dfs -rm /user/<your_username>/lab01/renamed.txt

# Delete directory and all contents
hdfs dfs -rm -r /user/<your_username>/lab01/temp
```

---

## 📊 Part 7: Cluster Metadata Analysis

### Step 7.1: Check Disk Usage

See how much space your files use:

```bash
# Your directory size
hdfs dfs -du -h /user/<your_username>

# Summary
hdfs dfs -du -s -h /user/<your_username>
```

**Expected:** ~300 MB (150 MB file × 2 replicas)

---

### Step 7.2: Count Files and Directories

```bash
hdfs dfs -count /user/<your_username>
```

**Output format:**
```
DIR_COUNT   FILE_COUNT   CONTENT_SIZE   PATHNAME
```

---

### Step 7.3: View Namespace Quota

Check your storage limit:

```bash
hdfs dfs -count -q /user/<your_username>
```

**Note:** You have a 300 MB quota per student.

---

## 🧪 Part 8: Advanced Exploration (Optional)

### Step 8.1: Examine Block Details

For a specific block:

```bash
hdfs fsck /user/<your_username>/lab01/bigfile.dat -blockId <block_id>
```

Get the `block_id` from the previous `fsck` command output.

---

### Step 8.2: Check Replication Health

See if any blocks are under-replicated:

```bash
hdfs fsck / -list-corruptfileblocks
```

**Expected:** Permission denied (students don't have access to run fsck on root).

**Alternative - Check your own files:**
```bash
hdfs fsck /user/<your_username> -list-corruptfileblocks
```

**Expected:** No corrupt blocks in your directory.

---

### Step 8.3: Explore Root Directory

⚠️ **Read-only!** You can view but not modify:

```bash
hdfs dfs -ls /
hdfs dfs -ls /user
```

See all students' directories (but you can't access their contents).

---

## ✅ Lab Completion Checklist

Mark these as complete:

- [ ] Successfully connected to cluster via SSH
- [ ] Viewed cluster status with `dfsadmin -report`
- [ ] Created directories in your HDFS home
- [ ] Uploaded a small text file
- [ ] Uploaded a large file (>128 MB)
- [ ] Examined file blocks and replication
- [ ] Performed file operations (copy, move, delete)
- [ ] Checked disk usage and metadata

---

## 📝 Lab Questions (Submit Answers)

1. **What is the total configured capacity of the cluster?**

2. **How many DataNodes are live in the cluster?**

3. **What is the default replication factor?**

4. **How many blocks does your 150 MB file have? Why?**

5. **If the replication factor is 2, how much space does a 100 MB file consume in total?**

6. **What happens if one DataNode fails? Will your data be lost? Why?**

---

## 🎓 Key Takeaways

1. **HDFS is a distributed filesystem** that splits files into blocks
2. **Default block size is 128 MB** - files larger than this span multiple blocks
3. **Replication provides fault tolerance** - each block exists on multiple nodes
4. **Metadata is managed by the NameNode** - it tracks where blocks are stored
5. **DataNodes store the actual data** - they report to the NameNode

---

## 🆘 Troubleshooting

**Problem:** Can't connect via SSH  
**Solution:** Check your username and password. Contact instructor if issue persists.

**Problem:** "Permission denied" error  
**Solution:** You can only write to `/user/<your_username>`. Don't try to write elsewhere.

**Problem:** "No space left" error  
**Solution:** You've exceeded your 300 MB quota. Delete some files.

**Problem:** File upload is very slow  
**Solution:** Normal for large files. The 150 MB file may take 30-60 seconds.

---

## 📚 Additional Resources

- [HDFS Commands Cheat Sheet](https://github.com/aniskoubaa/big_data_course)
- Course Slides: Week 2B & 2C
- Textbook: Chapter 2 - Distributed Storage with HDFS

---

**Congratulations!** 🎉 You've completed your first HDFS lab!

**Next Steps:** Explore more HDFS commands and prepare for the MapReduce lab in Week 3.
