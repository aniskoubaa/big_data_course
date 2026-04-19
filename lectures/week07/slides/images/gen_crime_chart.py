"""
Generate horizontal bar chart of top 10 crime types (Chicago Crimes dataset).

The counts below are SIMULATED values representative of the Chicago Crimes
open dataset (data.cityofchicago.org).  They mirror the distribution in a
10,000-row sample:
  THEFT ≈ 21%, BATTERY ≈ 19%, CRIMINAL DAMAGE ≈ 9%, etc.

These are for classroom illustration only.  Students reproduce real counts
from the actual dataset in their lab exercises.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

crime_types = ['THEFT', 'BATTERY', 'CRIMINAL\nDAMAGE', 'NARCOTICS',
               'ASSAULT', 'OTHER\nOFFENSE', 'BURGLARY',
               'MOTOR\nVEHICLE\nTHEFT', 'ROBBERY', 'DECEPTIVE\nPRACTICE']
counts = [2124, 1870, 920, 712, 650, 510, 395, 380, 320, 285]

fig, ax = plt.subplots(figsize=(11, 5))
colors = ['#6360FF' if i < 3 else '#00D4FF' if i < 6
          else '#32D583' for i in range(len(crime_types))]
bars = ax.barh(range(len(crime_types)-1, -1, -1), counts,
               color=colors, alpha=0.85, edgecolor='white')

ax.set_yticks(range(len(crime_types)-1, -1, -1))
ax.set_yticklabels(crime_types, fontsize=10)
ax.set_xlabel('Number of Crimes', fontsize=12)
ax.set_title('Top 10 Crime Types — Chicago Crimes Dataset',
             fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)

for bar, c in zip(bars, counts):
    ax.text(bar.get_width() + 20,
            bar.get_y() + bar.get_height()/2,
            f'{c:,}', va='center', fontsize=10,
            fontweight='bold', color='#0A2540')

ax.set_xlim(0, max(counts) * 1.15)
plt.tight_layout()
plt.savefig('crime_counts_bar.pdf', bbox_inches='tight', dpi=150)
plt.savefig('crime_counts_bar.png', bbox_inches='tight', dpi=150)
print("crime_counts_bar.pdf saved.")
