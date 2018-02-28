import sys
sys.path.extend((".","..","code"))
from local_settings import * ### for codePath dataPath psqlPath

import numpy as np
import pandas as pd

import psycopg2
from numpy import array
from scipy.spatial import cKDTree, Voronoi, voronoi_plot_2d
from scipy.cluster.vq import kmeans2, whiten

import pyBall

"""
From ball data produce a discrretization of the court
"""

### function for finding centroids from ball positions
###     draw ball points from db
###     draw all ball points from db
###     draw all ball points from db below 7 feet
###     build clusters
def computeDiscretization(nclusters = 8):
    """
    This uses kmeans to cluster the court into roughly equiprobable regions.

    It takes no arguments are returns kmeans centroids
    """

    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT x, y FROM coordinates WHERE pid = -1 AND z <= 8")
        #cur.execute("SELECT x, y FROM gamestate WHERE pid = -1 AND z <= 8")
        coordinates = np.array(cur.fetchall())
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)

    minit = np.array(list(zip(np.repeat([18.8, 37.6, 56.4, 75.2],2), np.repeat([16.667,33.333],4 ))))
    cent, code = kmeans2(coordinates[['x','y']],k=minit,minit='matrix')
    ### confirm equal cluster sizes
    return( cent )

if False:
    ### compute discretization
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    ### SLOW
    con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
    cur = con.cursor()
    cur.execute("SELECT x, y FROM coordinates WHERE z <= 8")
    #cur.execute("SELECT x, y FROM gamestate WHERE pid = -1 AND z <= 8")
    bl = pd.DataFrame(cur.fetchall())
    bl.columns = [d.name for d in cur.description]
    ### symmetrize along middle and just cluster halfcourt (for less drift from minit to centroids)
    #minit = np.array(list(zip(np.repeat([18.8, 37.6, 56.4, 75.2],2), np.repeat([16.667,33.333],4 ))))
    #minit = np.array(list(zip(np.repeat([20, 74],4), np.repeat([10,20,30,40],2 ))))
    #minit = np.array([[25,5],[25,40],[15,30],[35,30],[15,64],[35,64],[25,54],[25,89]])
    bl = bl.assign(bit2 = np.where(bl.x <= 94/2, 0, 1))
    bl = bl.assign(bit3 = np.where(bl.y <= 50/2, 0, 1))
    ### calculate ball within or beyond 3 point line.
    ###  33pt line is a 23.75 foot part-circle on top of a 22x14.2 foot rectangle:
    ###       22*tan(acos(22/23.75)) + 4 + 9/12 = 13.698
    bl = bl.assign(xsym = np.where(bl.x <= 94/2, bl.x, (94-bl.x)))
    bl = bl.assign(ysym = np.where(bl.y <= 50/2, bl.y, (50-bl.y)))
    bl = bl.assign(bit1 = 0)
    bl = bl.assign(bit1 = np.where((bl.ysym > 3) & (bl.xsym <= 14.2), 1, bl.bit1))
    bl = bl.assign(bit1 = np.where((bl.ysym > 3) & (bl.xsym > 14.2) & (bl.xsym < ((23.75**2 - (bl.ysym - 25)**2)**0.5 + 5.25)), 1, bl.bit1))
    bl = bl.assign(state = 2**0*bl.bit1 + 2**1*bl.bit2 + 2**2*bl.bit3)

    if False:
        #minit = np.array([[25,5],[25,40],[15,30],[35,30]])
        #minit = np.array([[5,25],[40,25],[30,15],[30,35]])
        #minit = np.array([[0,0],[47,25]])
        #minit = np.array([[0,25],[47,0]])
        minit = np.array([[15,25],[47,0]])
        nclusters = len(minit)
        cent, code = kmeans2(ballpt[['x','y']],k=minit,minit='matrix')
        ballpt = ballpt.assign(state = code)
        ### confirm equal cluster sizes
        print(cent)
        print(np.bincount(code))
        ### on 2014 data: [1900487 2176462 2768822 2463017 2948497 2324294 2206209 1895440]
        #cent = computeDiscretization(nclusters = nclusters)
        cent.dump(dataPath + "ball_centroids.pickle") ### save points
        ### plot
        vor = Voronoi(cent)
        colors = cm.rainbow(np.linspace(0, 1, nclusters))
        _ballpt = ballpt.sample(100000)
        ### plot
        voronoi_plot_2d(vor)
        plt.xlim(0, 47); plt.ylim(0, 25)
        for i, c in zip(range(nclusters), colors):
            plt.scatter(_ballpt.query('state == {}'.format(i))[['x']], _ballpt.query('state == {}'.format(i))[['y']], color=c, s=0.1)
            plt.scatter(cent[1], cent[0], c='r')
        plt.show()
    nclusters = 8
    colors = cm.rainbow(np.linspace(0, 1, nclusters))
    _ballpt = bl.sample(100000)
    ### plot
    circle = plt.Circle((5.25, 25), radius=23.75, fc='y')
    rectangle = plt.Rectangle((0, 3), 14.2, 44, fc='r')
    plt.xlim(0, 47); plt.ylim(0, 25)
    plt.gca().add_patch(circle)
    plt.gca().add_patch(rectangle)
    for i, c in zip(range(nclusters), colors):
        plt.scatter(_ballpt.query('state == {}'.format(i))[['x']], _ballpt.query('state == {}'.format(i))[['y']], color=c, s=0.1)
        #plt.scatter(cent[1], cent[0], c='r')

    plt.show()
    ### lookups
    file_name = dataPath + "nbagame0021400377.json.gz"
    coordinates = pyBall.all_position_data_load( pyBall.json_from_filename(file_name) )
    coordinates = coordinates.assign(bit2 = np.where(coordinates.x <= 94/2, 0, 1))
    coordinates = coordinates.assign(bit3 = np.where(coordinates.y <= 50/2, 0, 1))
    ### calculate ball within or beyond 3 point line.
    ###  33pt line is a 23.75 foot part-circle on top of a 22x14.2 foot rectangle:
    ###       22*tan(acos(22/23.75)) + 4 + 9/12 = 13.698
    coordinates = coordinates.assign(xsym = np.where(coordinates.x <= 94/2, coordinates.x, (94-coordinates.x)))
    coordinates = coordinates.assign(ysym = np.where(coordinates.y <= 50/2, coordinates.y, (50-coordinates.y)))
    coordinates = coordinates.assign(bit1 = 0)
    coordinates = coordinates.assign(bit1 = np.where((coordinates.ysym > 3) & (coordinates.xsym <= 14.2), 1, coordinates.bit1))
    coordinates = coordinates.assign(bit1 = np.where((coordinates.ysym > 3) & (coordinates.xsym > 14.2) & (coordinates.xsym < ((23.75**2 - (coordinates.ysym - 25)**2)**0.5 + 5.25)), 1, coordinates.bit1))
    coordinates = coordinates.assign(state = 2**0*coordinates.bit1 + 2**1*coordinates.bit2 + 2**2*coordinates.bit3)
    ### add discretizations
    ###     map players to regions
    #cent = np.load(dataPath + "ball_centroids.pickle") ### function for loading centroids
    #voronoi_kdtree = cKDTree(cent)
    #point_distances, point_states = voronoi_kdtree.query(coordinates[['x','y']], k=1)
    #coordinates = coordinates.assign(state = point_states)
    ### test discretization by plot
    ### plot points (and voronoi ridges)
    #vor = Voronoi(cent)
    #voronoi_plot_2d(vor)
    plt.xlim(0, 94); plt.ylim(0, 50)
    colors = cm.rainbow(np.linspace(0, 1, nclusters))
    for i, c in zip(range(nclusters), colors):
        plt.scatter(coordinates.query('state == {}'.format(i))[['x']], coordinates.query('state == {}'.format(i))[['y']], color=c, s=0.1)
        #plt.scatter(cent[1], cent[0], c='r')

    plt.show()




if __name__ == '__main__' and False:
    file_name = dataPath + "nbagame0021400377.json.gz"
    coordinates = pyBall.all_position_data_load( file_name )
    ### pebl reocvering strategy
    #gg = np.array(coordinates.head())
    #maximum_entropy_discretize(gg, includevars=['x','y','z'], excludevars=[], numbins=3)
    ### Voronoi strategy
    from scipy.spatial import Voronoi, voronoi_plot_2d
    import matplotlib.pyplot as plt
    vor = Voronoi(coordinates[['x','y']])
    voronoi_plot_2d(vor)
    plt.show()
    ### scipy binning strategy
    from scipy import stats
    ret = stats.binned_statistic_2d(coordinates.x[0:1000], coordinates.y[0:1000], None, 'count', range=[[0,94],[0,50]], bins=4, expand_binnumbers=True)
    ret.statistic
    ### kmeans strategy
    from numpy import array
    from scipy.cluster.vq import vq, kmeans, kmeans2, whiten
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    nclusters = 8
    coord = coordinates.iloc[:10000,]
    minit = np.array(list(zip(np.repeat([18.8, 37.6, 56.4, 75.2],2), np.repeat([16.667,33.333],4 ))))
    cent, code = kmeans2(coord[['x','y']],k=minit,minit='matrix')
    ### plot
    vor = Voronoi(cent)
    voronoi_plot_2d(vor)
    plt.xlim(0, 94); plt.ylim(0, 50)
    colors = cm.rainbow(np.linspace(0, 1, nclusters))
    for i, c in zip(range(nclusters), colors):
        plt.scatter(coord[code == i][['x']], coord[code == i][['y']], color=c)
        plt.scatter(cent[1], cent[0], c='r')
        #print(coord[code == i][['x','y']].shape)
    #for simplex in vor.ridge_vertices:
        #simplex = np.asarray(simplex)
        #plt.plot(vor.vertices[simplex,0], vor.vertices[simplex,1], 'k-')
    plt.show()
    ### recover region given point
    from scipy.spatial import tsearch
    tsearch(vor, [0,0])
    tsearch(vor, [40,90])
    tsearch(vor, [20,42])
    from scipy.spatial import cKDTree
    test_points = [ [0,0] , [40,90] , [20,42]]
    voronoi_kdtree = cKDTree(cent)
    test_point_dist, test_point_regions = voronoi_kdtree.query(test_points, k=1)
    ### function for finding centroids from ball positions
    ###     draw ball points from db
    ###     draw all ball points from db
    ###     draw all ball points from db below 7 feet
    ###     build clusters
    ### plot points (and voronoi ridges)
    ### save points
    ### function for loading centroids
    ### function for calculating distances
    ###     map players to regions
    ### integrate discretization into db inputs

