#!/usr/bin/env python3
"""
SE446 Lab 07 — Hadoop Streaming Reducer
=========================================
Reads sorted "word\tcount" pairs from stdin (Hadoop guarantees they arrive
sorted by key). Aggregates counts per word and emits the final total.

Because Hadoop sorts the mapper output before sending it to the reducer,
we only need to track the *current* word and sum — no hash map required.
This is the classic O(n) streaming reduce pattern.
"""

import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    word, count_str = parts
    try:
        count = int(count_str)
    except ValueError:
        continue

    if word == current_word:
        # Same key — keep accumulating
        current_count += count
    else:
        # New key — emit the previous word's total
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# Emit the last word
if current_word is not None:
    print(f"{current_word}\t{current_count}")
