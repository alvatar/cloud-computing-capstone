#!/usr/bin/env python

import sys
import pandas as pd
import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt
import numpy as np


df = pd.read_csv(sys.argv[1]);

df.columns = ['Rank', 'Frequency']
ax = df.plot(loglog=True, kind='line',x='Rank',y='Frequency')

fig = ax.get_figure()
fig.savefig('airport_popularity.png')
