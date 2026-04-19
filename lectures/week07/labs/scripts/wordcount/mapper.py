#!/usr/bin/env python3
"""
SE446 Lab 07 — Hadoop Streaming Mapper
========================================
Reads lines from stdin (one line per record, as sent by Hadoop).
Emits one tab-separated "word\t1" pair per word token.

Hadoop Streaming passes each HDFS split as stdin to this script.
The output goes to the Hadoop shuffle/sort phase automatically.
"""

import sys
import re

# Pre-compile regex once for efficiency
WORD_PATTERN = re.compile(r"[a-z_0-9]+")

for line in sys.stdin:
    line = line.strip().lower()
    if not line:
        continue
    for word in WORD_PATTERN.findall(line):
        # Emit: key=word, value=1
        print(f"{word}\t1")
