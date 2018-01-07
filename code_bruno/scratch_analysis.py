import sys
sys.path.append("..")
from local_settings import *
import requests
import pandas as pd
import numpy as np
import json

# uncomment this line for inline use with ipython on mac
#%matplotlib osx

import matplotlib.pyplot as plt
import seaborn as sns
import nba_py as nba
from nba_py import player
from nba_py import team
from scipy.spatial.distance import euclidean
from sklearn.neighbors import KernelDensity
from dataloader import data_loader

import sys
# sys.path.insert(0, './code')
# import dataloader
# import utils
# import 'code/utils'


#df = data_loader('./data/0021500502.json')
df = data_loader(dataPath+'nbagame0021400077.json.gz')
#df = data_loader('~/Desktop/0021500281.json')


court = plt.imread("./code/fullcourt.png")


# create player masks
sa_mask = (df.team_id == 1610612759) & (df.quarter == 1)
sa_spurs = df[sa_mask]

## Test block for one player


#### PLOT THE DISTRIBUTION FOR LEONARD
triggers = ["Kawhi Leonard", "Tim Dunca" "Andre Miller", "Matt Barnes"]


plot_distribution(sa_spurs, "Manu Ginobili", 2)
plot_distribution(sa_spurs, "Boris Diaw", 2)


leonard = sa_spurs[sa_spurs.player_name == "Kawhi Leonard"]
tim = sa_spurs[sa_spurs.player_name == "Tim Duncan"]

mutual_information((tim[["x_loc", "y_loc"]], leonard[["x_loc", "y_loc"]]))
ball[["x_loc", "y_loc"]]





time_mask = (df.game_clock <= 706) & (df.game_clock >= 702) & \
            (df.shot_clock <= 10.1) & (df.shot_clock >= 6.2)
time_df = df[time_mask]


# plot player masks
# Boolean mask to get the players we want
player_mask = (time_df.player_name=="Kawhi Leonard") | \
              (time_df.player_name=="Boban Marjanovic") | \
              (time_df.player_name=="Andre Miller") | \
              (time_df.player_name=="Matt Barnes")

group2 = sa_spurs[player_mask].groupby('player_name')[["x_loc", "y_loc"]]

# Get the differences in distances that we want
leoanard_marjanovic = player_dist(group2.get_group("Kawhi Leonard"),
                            group2.get_group("Boban Marjanovic"))
leonard_barnes = player_dist(group2.get_group("Kawhi Leonard"),
                            group2.get_group("Matt Barnes"))
leonard_miller = player_dist(group2.get_group("Kawhi Leonard"),
                            group2.get_group("Andre Miller"))

distances = [leoanard_marjanovic, leonard_barnes, leonard_miller]
labels = ["Ariza - Barnes", "Ariza - Paul", "Harden - Jordan"]

colors = sns.color_palette('colorblind', 3)

plt.figure(figsize=(12,9))

# Use enumerate to index the labels and colors and match
# them with the proper distance data
for i, dist in enumerate(distances):
    plt.plot(time_df.shot_clock.unique(), dist, color=colors[i])
    
    y_pos = dist[-1]
    
    plt.text(6.15, y_pos, labels[i], fontsize=14, color=colors[i])

# Create horizontal grid lines
plt.grid(axis='y',color='gray', linestyle='--', lw=0.5, alpha=0.5)

plt.xlim(10.1, 6.2)

plt.title("The Distance (in feet) Between Players \nFrom the Beginning"
          " of Harden's Drive up until Ariza Releases his Shot", size=16)
plt.xlabel("Time Left on Shot Clock (seconds)", size=14)

# Get rid of unneeded chart lines
sns.despine(left=True, bottom=True) 

plt.show()




