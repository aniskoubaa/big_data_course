"""
Generate performance comparison chart: MapReduce vs Spark.

The execution times below are ILLUSTRATIVE estimates based on published
benchmarks (Zaharia et al., NSDI 2012; Spark project benchmarks) and are
rounded for classroom clarity:

  - Word Count (1-pass):  MR ~120 s, Spark ~100 s  →  ~1.2× (single-pass
    jobs show little benefit because there is no iterative reuse of data).
  - PageRank (10 iter):   MR ~950 s, Spark ~95 s   → ~10×  (iterative graph
    algorithm; Spark keeps the graph in RAM across iterations).
  - K-Means (30 iter):    MR ~2400 s, Spark ~80 s  → ~30×  (each iteration
    re-reads centroids from HDFS in MR; Spark caches them in memory).
  - Logistic Regression (10 iter): MR ~1800 s, Spark ~60 s → ~30×
    (classic benchmark from the original Spark paper).

Sources:
  [1] Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant
      Abstraction for In-Memory Cluster Computing," NSDI 2012.
  [2] Apache Spark project benchmarks (spark.apache.org).
  [3] Databricks blog, "Spark vs MapReduce" (2014, 2018 updates).
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

tasks = ['Word Count\n(1-pass)', 'PageRank\n(10 iter)',
         'K-Means\n(30 iter)', 'Logistic Reg.\n(10 iter)']
mr_times = [120, 950, 2400, 1800]
spark_times = [100, 95, 80, 60]

x = np.arange(len(tasks))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(x - width/2, mr_times, width,
       label='Hadoop MapReduce', color='#DC3545', alpha=0.85)
ax.bar(x + width/2, spark_times, width,
       label='Apache Spark', color='#6360FF', alpha=0.85)

ax.set_ylabel('Execution Time (seconds)', fontsize=13)
ax.set_title('MapReduce vs Spark: Execution Time Comparison',
             fontsize=15, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(tasks, fontsize=11)
ax.legend(fontsize=12)
ax.set_yscale('log')
ax.set_ylim(10, 5000)
ax.grid(axis='y', alpha=0.3)

for i, (mr, sp) in enumerate(zip(mr_times, spark_times)):
    speedup = mr / sp
    ax.annotate(f'{speedup:.0f}x faster',
                xy=(x[i] + width/2, sp),
                xytext=(0, 15),
                textcoords='offset points',
                ha='center', fontsize=10, fontweight='bold',
                color='#6360FF')

plt.tight_layout()
plt.savefig('spark_vs_mr_performance.pdf', bbox_inches='tight', dpi=150)
plt.savefig('spark_vs_mr_performance.png', bbox_inches='tight', dpi=150)
print("spark_vs_mr_performance.pdf saved.")
