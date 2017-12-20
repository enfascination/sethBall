exec(open("local_settings.py").read()) ### for codePath dataPath psqlPath
import json
import numpy as np
import os
import tqdm
import matplotlib
from scipy.ndimage.filters import gaussian_filter as smooth
import pyBall

matplotlib.rcParams["mathtext.fontset"] = "stix"
matplotlib.rcParams['font.family'] = 'STIXGeneral'

if True:
    #coordinates = pyBall.ball_phase_space_generate(dataPath)
    print( pyBall.db_size() )
    coordinates_new = pyBall.ball_phase_space_generate_db(1000) ### enter number of observations desired


#----------------------------------- 1d Marginals 
hx = np.histogram(coordinates[0],bins = 940,normed=True)
hy = np.histogram(coordinates[1],bins = 500,normed=True)
hz = np.histogram(coordinates[2],bins = 1000,normed=True)

fig = plt.figure(n,figsize = (12,10))
ax1 = plt.axes([.08,.08,.4,.25])
plt.xlabel('$z$(ft)')
ax1.plot(hz[1][1:],hz[0])
ax2 = plt.axes([.08,.08+.06+.25,.4,.25])
plt.xlabel('$y$(ft)')
ax2.plot(hy[1][1:],hy[0])
ax3 = plt.axes([.08,.08+.06+.25+.06+.25,.4,.25])
plt.xlabel('$x$(ft)')
ax3.plot(hx[1][1:],hx[0])
ax3.set_title('Position Marginals')
ax4 = plt.axes([.08+.48,.08,.4,.25])
plt.xlabel('$v_z$')
ax4.plot(hvz[1][1:],hvz[0])
ax5 = plt.axes([.08+.48,.08+.06+.25,.4,.25])
plt.xlabel('$v_y$')
ax5.plot(hvy[1][1:],hvy[0])
ax6 = plt.axes([.08+.48,.08+.06+.25+.06+.25,.4,.25])
plt.xlabel('$v_x$')
ax6.plot(hvx[1][1:],hvx[0])
ax6.set_title('Velocity Marginals')

#----------------------------------- 2d Marginals
h = map(list,np.ones([6,6]))
for col in range(6):
    for row in range(col+1):
        if row == col:
            h[row][col] = np.histogram(coordinates[row],bins = 1000,normed=True)
        else:
            h[row][col]=np.histogram2d(coordinates[row],coordinates[col],bins = (1000,1000),normed = True)

fig = plt.figure(1,figsize = (12,12))
labels = ['$x$','$y$','$z$','$v_x$','$v_y$','$v_z$']
plt.clf()
for col in range(6):
    for row in range(col+1):
        print(row,col)
        ax = plt.axes([.05+col*.15,.05+row*.15,.15,.15])
        if row == col:
            x = h[row][col][1][1:]
            y = h[row][col][0]
            ax.plot(x,y,linewidth=.5)
            plt.ylabel(labels[row],rotation = 0)
        else:
            z = h[row][col][0]
            z[z==0] = min(z[z>0])
            z = smooth(z,10)
            ax.contour(np.log(z),100,linewidths = .2,linestyles='solid',colors = 'black',alpha = .5)
            ax.imshow(z,alpha = .8)
        ax.set_xticks([])
        ax.set_yticks([])
        if row==0:
            plt.xlabel(labels[col])

#-------------------------------------- Kinetic Energy

            
        
