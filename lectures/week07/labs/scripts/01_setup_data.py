#!/usr/bin/env python3
"""
SE446 Lab 07 — Data Generator
==============================
Generates a large synthetic text corpus (~150 MB) and uploads it to HDFS.

The corpus simulates a real-world text dataset by mixing:
  - Common English words (high frequency)
  - Technical / domain-specific words (medium frequency)
  - Rare words (long tail distribution)

This frequency distribution matters: it means the filtering steps
(words >= 100 occurrences) will cut down the result set substantially,
making jobs 2 and 3 meaningfully different from job 1.

Usage (on master-node):
    python3 01_setup_data.py [--size-mb SIZE] [--hdfs-path PATH]
"""

import random
import subprocess
import sys
import os
import argparse
import time

# ─── Word pools ─────────────────────────────────────────────────────────────

# Very common words (will appear thousands of times)
COMMON = [
    "the", "of", "and", "to", "a", "in", "is", "it", "you", "that",
    "he", "was", "for", "on", "are", "with", "as", "at", "be", "or",
    "from", "this", "have", "an", "by", "not", "but", "had", "his", "they",
    "we", "been", "which", "she", "do", "their", "all", "if", "more", "will",
    "so", "one", "can", "has", "her", "there", "what", "when", "who", "said",
    "data", "node", "cluster", "job", "task", "file", "system", "process",
    "map", "reduce", "spark", "hadoop", "rdd", "hdfs", "yarn", "partition",
]

# Medium-frequency technical words
MEDIUM = [
    "distributed", "parallel", "compute", "executor", "driver", "worker",
    "memory", "cache", "shuffle", "stage", "action", "transformation",
    "fault", "tolerant", "lineage", "scheduler", "pipeline", "replicate",
    "block", "chunk", "stream", "batch", "query", "aggregate", "filter",
    "count", "sort", "join", "group", "key", "value", "pair", "tuple",
    "latency", "throughput", "bandwidth", "network", "disk", "cpu", "core",
    "container", "application", "context", "session", "configuration",
    "master", "slave", "leader", "follower", "zookeeper", "namenode",
    "datanode", "resource", "manager", "appmaster", "timeline", "history",
]

# Rare words (long tail — will appear < 100 times in large corpus)
RARE = [
    "skewness", "predicate", "pushdown", "tungsten", "catalyst", "codegen",
    "vectorized", "columnar", "parquet", "avro", "orc", "kryo", "serializer",
    "speculation", "blacklist", "heartbeat", "rendezvous", "checkpoint",
    "watermark", "backpressure", "microbatch", "structured", "unmanaged",
    "coalesce", "repartition", "broadcast", "accumulator", "closure",
    "kryo", "netty", "thrift", "avro", "protobuf", "arrow", "gandiva",
] + [f"word_{i}" for i in range(200, 500)]


def weighted_word():
    """Return a word sampled from the weighted distribution."""
    r = random.random()
    if r < 0.60:          # 60% — common words
        return random.choice(COMMON)
    elif r < 0.90:        # 30% — medium words
        return random.choice(MEDIUM)
    else:                 # 10% — rare words
        return random.choice(RARE)


def generate_line():
    """Generate one line of 8–20 words."""
    n = random.randint(8, 20)
    return " ".join(weighted_word() for _ in range(n))


def generate_corpus(output_path: str, target_mb: int = 150):
    """Write the corpus file until we reach the target size."""
    target_bytes = target_mb * 1024 * 1024
    written = 0
    lines_written = 0
    bar_width = 40
    t0 = time.time()

    print(f"Generating ~{target_mb} MB corpus at: {output_path}")
    with open(output_path, "w") as f:
        while written < target_bytes:
            line = generate_line()
            f.write(line + "\n")
            written += len(line) + 1
            lines_written += 1
            if lines_written % 100_000 == 0:
                pct = min(written / target_bytes, 1.0)
                filled = int(bar_width * pct)
                bar = "#" * filled + "-" * (bar_width - filled)
                mb_done = written / 1024 / 1024
                elapsed = time.time() - t0
                print(f"\r  [{bar}] {mb_done:6.1f}/{target_mb} MB  "
                      f"({lines_written:,} lines, {elapsed:.0f}s)", end="", flush=True)

    elapsed = time.time() - t0
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n  Done in {elapsed:.1f}s — {size_mb:.1f} MB, {lines_written:,} lines")
    return output_path


def upload_to_hdfs(local_path: str, hdfs_path: str):
    """Create HDFS directory and upload the file."""
    # Ensure HDFS directory exists
    hdfs_dir = os.path.dirname(hdfs_path)
    print(f"\nCreating HDFS directory: {hdfs_dir}")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)

    print(f"Uploading to HDFS: {hdfs_path}")
    t0 = time.time()
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
    elapsed = time.time() - t0
    print(f"  Uploaded in {elapsed:.1f}s")

    # Verify
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "-h", hdfs_path],
        capture_output=True, text=True
    )
    print(f"  HDFS listing:\n  {result.stdout.strip()}")

    # Check replication
    result = subprocess.run(
        ["hdfs", "dfs", "-stat", "%r", hdfs_path],
        capture_output=True, text=True
    )
    print(f"  Replication factor: {result.stdout.strip()}")


def main():
    parser = argparse.ArgumentParser(description="SE446 Lab 07 data generator")
    parser.add_argument("--size-mb", type=int, default=150,
                        help="Target file size in MB (default: 150)")
    parser.add_argument("--hdfs-path", default=None,
                        help="HDFS destination path (overrides --student)")
    parser.add_argument("--local-path", default="/tmp/lab07_corpus.txt",
                        help="Local temp file path")
    parser.add_argument("--student", default=None,
                        help="Student username — sets HDFS path to /se446/lab07/<student>/corpus.txt")
    args = parser.parse_args()

    # Resolve HDFS path: --hdfs-path wins; else --student; else current user home
    if args.hdfs_path is None:
        if args.student:
            args.hdfs_path = f"/user/{args.student}/lab07/corpus.txt"
        else:
            import getpass
            current_user = getpass.getuser()
            args.hdfs_path = f"/user/{current_user}/lab07/corpus.txt"

    print("=" * 60)
    print("  SE446 Lab 07 — Data Setup")
    print("=" * 60)

    # Step 1: Generate
    generate_corpus(args.local_path, args.size_mb)

    # Step 2: Upload
    upload_to_hdfs(args.local_path, args.hdfs_path)

    print("\n" + "=" * 60)
    print("  Setup complete!")
    print(f"  Dataset: {args.hdfs_path}")
    print("=" * 60)
    print("\nNext step: run the MapReduce benchmark:")
    print("  bash scripts/02_run_mapreduce.sh\n")


if __name__ == "__main__":
    main()
