"""
SE446 Lab 07 — Spark Benchmark: Three Actions on a Cached RDD
=================================================================
Performs the SAME three analyses as the MapReduce benchmark, but
using Apache Spark with RDD caching.

The key difference:
  - The corpus is read from HDFS exactly ONCE (first action)
  - After .cache(), the RDD lives in executor RAM
  - Actions 2 and 3 NEVER touch the disk again — pure in-memory

Analyses (matching MapReduce jobs exactly):
  Action 1: Total word count  (compare to MR Job 1)
  Action 2: Count of words appearing >= 100 times  (compare to MR Job 2)
  Action 3: Top 10 most frequent words  (compare to MR Job 3)

Usage:
  /opt/spark/bin/spark-submit \\
      --master yarn \\
      --num-executors 2 \\
      --executor-memory 1g \\
      --executor-cores 1 \\
      scripts/03_run_spark.py

  # Or with Spark Standalone:
  /opt/spark/bin/spark-submit \\
      --master spark://master-node:7077 \\
      scripts/03_run_spark.py
"""

import re
import time
import sys

from pyspark import SparkContext, SparkConf

# ─── Configuration ────────────────────────────────────────────────────────────

# Accept optional positional argument: HDFS corpus path
# e.g.: spark-submit ... 03_run_spark.py /user/student01/lab07/corpus.txt
# Defaults to /user/<current_user>/lab07/corpus.txt
import getpass as _gp, os as _os
_default_user = _os.environ.get("USER", _gp.getuser())
_default_input = f"hdfs:///user/{_default_user}/lab07/corpus.txt"
if len(sys.argv) > 1:
    _arg = sys.argv[1]
    # Allow both hdfs:// and bare /path formats
    HDFS_INPUT = _arg if _arg.startswith("hdfs://") else f"hdfs://{_arg}"
else:
    HDFS_INPUT = _default_input

MIN_COUNT    = 100     # threshold for Action 2
TOP_N        = 10      # number of top words for Action 3
WORD_PATTERN = re.compile(r"[a-z_0-9]+")

# ─── Spark Setup ──────────────────────────────────────────────────────────────

def create_spark_context():
    conf = (SparkConf()
            .setAppName("SE446-Lab07-Spark-Benchmark")
            .set("spark.ui.port", "4040"))
    return SparkContext(conf=conf)


# ─── Helper ───────────────────────────────────────────────────────────────────

def banner(title):
    line = "=" * 60
    print(f"\n{line}")
    print(f"  {title}")
    print(line)


def timed(label, fn):
    """Run fn(), print elapsed time, return (result, elapsed_seconds)."""
    print(f"\n  Running: {label} ...", flush=True)
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  ⏱  Done in {elapsed:.2f}s")
    return result, elapsed


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    banner("SE446 Lab 07 — Spark Benchmark")
    print(f"  Input: {HDFS_INPUT}")

    sc = create_spark_context()
    sc.setLogLevel("WARN")   # suppress INFO noise in student output

    # ── Load and cache the RDD ────────────────────────────────────────────────
    print("\n  Loading corpus from HDFS and building word-count RDD...")
    print("  NOTE: data will be cached in executor RAM after the first action.\n")

    # Tokenise: read lines → split into words → map to (word, 1) → reduce by key
    # The result (word_counts) is a PairRDD: (word, total_count)
    word_counts = (
        sc.textFile(HDFS_INPUT)
          .flatMap(lambda line: WORD_PATTERN.findall(line.lower()))
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a + b)
    )

    # Cache the word_counts RDD — this is the key Spark advantage.
    # After the first action materialises it, subsequent actions
    # work from executor RAM — no HDFS read.
    word_counts.cache()
    print("  RDD is lazily defined; .cache() registered.")
    print("  First action will trigger the actual HDFS read + computation.")

    # ── Action 1: Total distinct words ────────────────────────────────────────
    banner("Action 1: Total distinct word count")
    print("  (This triggers the HDFS read + wordcount computation;")
    print("   the result is cached in executor RAM for the next actions)")

    total_words, t1 = timed("count()",  word_counts.count)

    print(f"\n  Result: {total_words:,} distinct words in the corpus")
    print(f"  SPARK_ACTION1_SECONDS={t1:.2f}")

    # ── Action 2: Words appearing >= 100 times ────────────────────────────────
    banner(f"Action 2: Words with count >= {MIN_COUNT}")
    print("  (reading from CACHE — no HDFS I/O)")

    frequent_count, t2 = timed(
        f"filter(count >= {MIN_COUNT}).count()",
        lambda: word_counts.filter(lambda kv: kv[1] >= MIN_COUNT).count()
    )

    print(f"\n  Result: {frequent_count:,} words appear >= {MIN_COUNT} times")
    print(f"  SPARK_ACTION2_SECONDS={t2:.2f}")

    # ── Action 3: Top 10 most frequent words ──────────────────────────────────
    banner("Action 3: Top 10 most frequent words")
    print("  (reading from CACHE — no HDFS I/O)")

    top10, t3 = timed(
        "sortBy(count desc).take(10)",
        lambda: word_counts.sortBy(lambda kv: kv[1], ascending=False).take(TOP_N)
    )

    print(f"\n  {'Rank':<6} {'Word':<25} {'Count':>10}")
    print(f"  {'-'*6} {'-'*25} {'-'*10}")
    for rank, (word, count) in enumerate(top10, 1):
        print(f"  {rank:<6} {word:<25} {count:>10,}")
    print(f"\n  SPARK_ACTION3_SECONDS={t3:.2f}")

    # ── Cache Status ──────────────────────────────────────────────────────────
    banner("Cache Usage")
    status = sc.statusTracker()
    print(f"  Check http://master-node:4040/storage (if still running)")
    print(f"  or   http://master-node:18080 (History Server after completion)")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_spark = t1 + t2 + t3

    banner("SPARK BENCHMARK SUMMARY")
    print()
    print(f"  {'Action':<40} {'Seconds':>8}")
    print(f"  {'-'*50}")
    print(f"  {'Action 1: Total word count (cold read + cache)':<40} {t1:>8.2f}")
    print(f"  {'Action 2: Words >= 100 (from cache)':<40} {t2:>8.2f}")
    print(f"  {'Action 3: Top 10 (from cache)':<40} {t3:>8.2f}")
    print(f"  {'-'*50}")
    print(f"  {'TOTAL Spark':<40} {total_spark:>8.2f}")
    print()
    print(f"  SPARK_TOTAL_SECONDS={total_spark:.2f}")
    print()
    print("  Notice: Actions 2 and 3 are MUCH faster than Action 1.")
    print("  Those queries ran entirely in executor RAM — no HDFS access.")
    print()
    banner("Next Step")
    print("  Run the comparison script:")
    print("    bash scripts/04_compare.sh /tmp/mr_benchmark.log /tmp/spark_benchmark.log")
    print()

    sc.stop()


if __name__ == "__main__":
    main()
