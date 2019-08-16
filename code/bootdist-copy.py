############ Boostdist

import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath

import json
import numpy as np
import pandas as pd
import os
import tqdm
import matplotlib.pyplot as plt
import gzip
import psycopg2
from functools import partial
import datetime

import pyBall
from pyBall import (build_distribution, bootstrap_dist_from_dist)

import pickle

import pprint 

import statistics 
from statistics import mean

from scipy import stats 
import seaborn as sns

con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
cur = con.cursor()
cur.execute("SELECT MAX(row) FROM gamestate")
id_max = cur.fetchone()[0]
dist = build_distribution(10, id_max=id_max)
assert dist.sum() == 20
distfull = build_distribution(id_max=id_max)

std0=[]
std1=[]
std2=[]
std3=[]
std4=[]
std5=[]
std=[]
stdd=[]

alist=[]
def filearr(dist):
	
	filelist=['nbadist']*10
	for i in range(10):

		filelist[i] = filelist[i] + str(i) + '.sav'

		boot = bootstrap_dist_from_dist(dist)# call on function that returns the array to a variable boot.

		with open(filelist[i],'wb') as f: #writing to a file in binary mode
			pickle.dump(boot,f)
			#boot.dump(f)
		with open(filelist[i],'rb') as f: #reading a binary file
			x = pickle.load(f) #different x each time for each dist
			print(x)
			#dat = pd.DataFrame(np.reshape(x,(64,64)))
			#https://pythontic.com/numpy/ndarray/mean_std_var
			#print('shape:',x.shape,', dims:',x.ndim) #shape: (4, 4, 4, 4, 4, 4) , dims: 6
			#stats.describe(x)
			#since calculating the mean, we get the same value repetitively, lets try std/max instead for more variance
			"""
			max = np.amax(x)
			std.append(np.std(x)/max)
			std0.append(np.std(x,axis = 0)/max) # std based on each entity for all 10 dist / min
			std1.append(np.std(x,axis = 1)/max)
			std2.append(np.std(x,axis = 2)/max)
			std3.append(np.std(x,axis = 3)/max)
			std4.append(np.std(x,axis = 4)/max)
			std5.append(np.std(x,axis = 5)/max)
	#average of the std/max  for each entity
	s0 = np.mean(np.array(std0))
	stdd.append(s0)
	s1 = np.mean(np.array(std1))
	stdd.append(s1)
	s2 = np.mean(np.array(std2))
	stdd.append(s2)
	s3 = np.mean(np.array(std3))
	stdd.append(s3)
	s4 = np.mean(np.array(std4))
	stdd.append(s4)
	s5 = np.mean(np.array(std5))
	stdd.append(s5)

	#print(std) # 10 std values for each dist
	#print(stdd) # 6 avg std values for each entity across the 10 dist
			
filearr(distfull)
  sudo launchctl load -w /System/Library/LaunchDaemons/com.apple.locate.plist

df10 = pd.DataFrame(std)
df6 = pd.DataFrame(stdd)
print(df10)
print()
print(df6)

sns.set(style="whitegrid")
ax = sns.boxplot(data=df10)
plt.savefig('boxplot.png')
plt.show()
"""
filearr(distfull)





########################################################################



