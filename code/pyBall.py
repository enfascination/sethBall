#---------------------------------------------------LIBRARIES
import json
import numpy as np
import os
import tqdm
import matplotlib
import gzip
import psycopg2

matplotlib.rcParams["mathtext.fontset"] = "stix"
matplotlib.rcParams['font.family'] = 'STIXGeneral'
#---------------------------------------------------DATA HANDLING FUNCTIONS

def ball_phase_space_generate(directory_name):
    """Extracts all the points in phase space of the ball from 
    the files in the directory ( a season).  
    
    INPUT
    directory_name  name of the directory holding the json files
    
    OUTPUT
    """
    file_names = os.listdir(directory_name)
    coordinates = []
    for file_name in tqdm.tqdm(file_names):
        game_data = ball_data_load(directory_name + file_name)
        if len(game_data) > 0:
            for point in game_data:
                coordinates.append(point)
    return np.array(coordinates)

def ball_data_load(file_name):
    """Loads the phase-space coordinates, (x,y,z,vx,vy,vz,...), for all 
    the frames of a game.
    
    INPUT
    file_name       The location of the file being read
    
    OUTPUT
    coordinates     An array of arrays. The top layer is the contiguous trajectories.
                    The next layer is the array of phase space coordinates of the ball 
                    along that contiguous trajectory. 
    """
    coordinates = []
    #this is the prjector that picks out the time stamp and spatial coordinates of
    # the ball from the moment data
    
    try:
        #Check to make sure that the file is a json file
        if _check_json(file_name):
            json_strs = _get_json_str(file_name)
        elif _check_jsongz(file_name):
            json_strs = _get_json_str(file_name, gzipped=True)
        #This loop goes over all the contiguous trajectories in the game
        for json_str in json_strs:
            try:
                moments = json.loads(json_str)['moments']
                if len(moments)>5:
                    #this is where the data is projected into the coordinate list
                    trajectory = np.array(list(map(_coordinate_projection,moments))).T
                    #adds the velocity, acceleration, etc
                    trajectory = _add_velocity(trajectory)
                    for point in trajectory[1:].T:
                        if _validate_point(point):
                            coordinates.append(point)
            except ValueError:
                pass
    except IndexError:
        pass
    return np.array(coordinates)

def ball_phase_space_generate_db(n):
    """Extracts all the points in phase space of the ball from 
    the database built by running builddb.py
    
    INPUT
    n  the number of observations to extract
    
    OUTPUT
    coordinates  the coordinates object
    """
    coordinates = np.array([])
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT x, y, z, vz, vy, vz FROM coordinates LIMIT %s"%(n,))
        #https://pythonspot.com/python-database-postgresql/
        #while True:
            #row = cur.fetchone()
            #if row == None: break
        coordinates = np.array(cur.fetchall())
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    return( coordinates )

def db_size():
    """returns the number of entries in the database
    
    INPUT
    
    OUTPUT
    n  the number of entries in the database
    """
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM coordinates")
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
    return( cur.fetchone()[0] )


def _validate_point(point):
    #Validates a point by checking to make sure it is in the boundary of the court,
    #and that the speeds are not too large (which is a sign of an artifact coming from
    #the finite difference method)
    return ((point[0]<=94)&(point[0]>=0)&(point[1]<=50)&(point[1]>=0)&(point[2]>0)&
            (np.abs(point[3])<40)&(np.abs(point[4])<40)&(np.abs(point[5])<40))

def _coordinate_projection(moment):
    #Projects the (t,x,y,z) coordinates of the ball from the moment structure
    return np.array([moment[2],moment[5][0][2],moment[5][0][3],moment[5][0][4]])

def _check_json(file_name):
    #checks to see if the extension of a file is .json
    return file_name[-4:]=='json'

def _check_jsongz(file_name):
    #checks to see if the extension of a file is .json
    return file_name[-7:]=='json.gz'

def _get_json_str(file_name, gzipped=True):
    if not gzipped:
        json_file = open(file_name,'r')
    else:
        json_file = gzip.open(file_name,'rt')
    return json_file.read().split('\n')

def _add_velocity(trajectory):
    dt = np.convolve(trajectory[0],[-1,0,1],'valid')
    #valid indices are those in which the clock is running
    valid_idx = dt!=0
    dt = dt[valid_idx]
    #smooth the coordinates
    t = np.convolve(trajectory[0],[.25,.5,.25],'valid')[valid_idx]
    x = np.convolve(trajectory[1],[.25,.5,.25],'valid')[valid_idx]
    y = np.convolve(trajectory[2],[.25,.5,.25],'valid')[valid_idx]
    z = np.convolve(trajectory[3],[.25,.5,.25],'valid')[valid_idx]
    #central difference method for the velocity, quadratic error
    dx = np.convolve(trajectory[1],[-1,0,1],'valid')[valid_idx]
    dy = np.convolve(trajectory[2],[-1,0,1],'valid')[valid_idx]
    dz = np.convolve(trajectory[3],[-1,0,1],'valid')[valid_idx]
    vx = dx/dt
    vy = dy/dt
    vz = dz/dt
    return np.array([t,x,y,z,vx,vy,vz])
