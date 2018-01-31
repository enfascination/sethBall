#---------------------------------------------------LIBRARIES
import json
import numpy as np
import os
import tqdm
import matplotlib
import gzip
import psycopg2
from functools import partial

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
    """Loads the phase-space coordinates of the ball, 
    (x,y,z,vx,vy,vz,...), for all the frames of a game.
    
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
                    #trajectory = np.array(list(map(_coordinate_projection_ball,moments))).T
                    mapfunc = partial(_coordinate_projection_entity,ientity=0)
                    moments_valid = filter(lambda x: len(x[5])==11,moments)
                    trajectory = np.array(list(map(mapfunc,moments_valid))).T
                    #adds the velocity, acceleration, etc
                    trajectory = _clean_time(trajectory, itime=2)
                    trajectory = _add_velocity(trajectory[2:6])
                    for point in trajectory[:].T:
                        #print(point)
                        if _validate_point(point[1:], ball=True):
                            coordinates.append(point[1:])
            except ValueError:
                pass
    except IndexError:
        pass
    return np.array(coordinates)

def all_position_data_load(file_name):
    """Loads the phase-space coordinates, (gameid, playerid,t,x,y,z), for all 
    the frames of a game, for all entities (players and ball).
    
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
        t_end_last = 720
        p_last = 1
        for json_str in json_strs:
            try:
                moments = json.loads(json_str)['moments']
                if len(moments)>5:
                    #this is where the data is projected into the coordinate list
                    ### repeat this for each entity on the court
                    ### filter out invalid moments
                    moments_valid = list(filter(lambda x: 
                            (len(x[5])==11) and #those without 11 entities on the court
                            (x[2] != 720) and #those with t=720 
                                              #(very very beginning of period, known to be buggy)
                            ((x[2] < t_end_last) or (x[0] != p_last))
                                              #those with event overlaps 
                                              #(in the same period, and beginning before the last event ended)
                        , moments))
                    #moments_valid =  moments
                    ### reset the overlap tracker (assumes events are temporally ordered)
                    t_end_last = min([x[2] for x in moments_valid]) ## time that this event ended
                    p_last = moments[0][0] ### period of this event 
                                           ### (assumes that all events are within period)
                                           ### (don't sweat this using moments instead of moments_valid)
                    for itraj in range(11):
                        ### set up the coordinate project for this entity
                        ### at this instant, time becomes something that counts up instead of down
                        mapfunc = partial(_coordinate_projection_entity,ientity=itraj)
                        ### produce 2D array of all states over time
                        trajectory = np.array(list(map(mapfunc,moments_valid))).T
                        ### remove all instances of time freezing
                        trajectory = _clean_time(trajectory, itime=2)
                        for point in trajectory[:].T:
                            ## validity of an entity position differs for ball and players
                            if _validate_position(point[3:6], ball=(point[0] == -1)):
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


def _validate_point(point, ball=False):
    """Validates a point by checking to make sure its position and velocity are valid"""
    return (_validate_position(point, ball=ball) & _validate_velocity(point))

def _validate_position(point, ball=False):
    """Validates a point by checking to make sure it is in the boundary of the court."""
    if ball:
        return ((point[0]<=94)&(point[0]>=0)&(point[1]<=50)&(point[1]>=0)&(point[2]>0))
    else:
        return ((point[0]<=94)&(point[0]>=0)&(point[1]<=50)&(point[1]>=0)&(point[2]>=0))

def _validate_velocity(point):
    """Validates a point by checking
    #that the speeds are not too large (which is a sign of an artifact coming from
    #the finite difference method)"""
    return ((np.abs(point[3])<40)&(np.abs(point[4])<40)&(np.abs(point[5])<40))

def _coordinate_projection_ball(moment):
    #Projects the (t,x,y,z) coordinates of the ball from the moment structure
    # here is the structure of each moment[5][x] 
    # (each entity at the given instant of the given event):
    #          ["team_id", "player_id", "x_loc", "y_loc",
    #          "radius", "moment", "quarter", "game_clock", "shot_clock"]
    if False : ### this is always true: ball, when present, is always the first entity
        assert(moment[5][0][0] == -1 ) ### teamid of the ball is -1
        assert(moment[5][0][1] == -1 ) ### playerid of the ball is -1
    t = (720 - moment[2]) + (moment[0] - 1) * 12 * 60
    return np.array([t,moment[5][0][2],moment[5][0][3],moment[5][0][4]])

def _coordinate_projection_entity(moment, ientity):
    #Projects the (t,team,entity,x,y,z) coordinates of the ith entity from the moment structure
    # here is the structure of each moment[5][x] 
    # (each entity at the given instant of the given event):
    #          ["team_id", "player_id", "x_loc", "y_loc",
    #          "radius", "moment", "quarter", "game_clock", "shot_clock"]
    if False and ientity == 0: ### this is always true: ball, when present, is always the first entity
        assert moment[5][0][0] == -1, "%s"%(str(moment),)  ### teamid of the ball is -1
        assert moment[5][0][1] == -1, "%s"%(str(moment),)  ### playerid of the ball is -1
    t = (720 - moment[2]) + (moment[0] - 1) * 12 * 60
    return np.array([moment[5][ientity][0],moment[5][ientity][1],t,
                     moment[5][ientity][2],moment[5][ientity][3],moment[5][ientity][4]])

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

def _clean_time(trajectory, itime=0):
    """ given a 2d array and the its time index, 
    gets rid of all instances of time freezing: 
        just one measurement per time step, exce
        pt possibly the very last two"""
    dt = np.convolve(trajectory[itime],[-1,1],'valid')
    #valid indices are those in which the clock is running
    valid_idx = dt!=0
    valid_idx = np.append(valid_idx, True ) # keep last value
    return(trajectory[:,valid_idx])

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

def _add_velocity_old(trajectory):
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

if __name__ == '__main__':
    #np.set_printoptions(formatter={'float_kind':'{:f}'.format}, floatmode="maxprec")
    np.set_printoptions(formatter={'float_kind':'{:f}'.format})
    file_name = "/Users/sfrey/Desktop/projecto/research_projects/nba_tracking/sampledata/nbagame0021400377.json.gz"
    json_strs = _get_json_str(file_name, gzipped=True)
    coordinates = all_position_data_load( file_name )
    coordinates_old = ball_data_load( file_name )
    print(coordinates_old[0:5])
    print(coordinates[0:5])
    print()
    print()
    print(type(coordinates_old[0:5]))
    print(type(coordinates[0:5]))
