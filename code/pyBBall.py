#---------------------------------------------------LIBRARIES
import json
import numpy as np
import os
import tqdm
import matplotlib

matplotlib.rcParams["mathtext.fontset"] = "stix"
matplotlib.rcParams['font.family'] = 'STIXGeneral'
#---------------------------------------------------DATA HANDLING FUNCTIONS

def ball_data_season_extractor(directory_name,order = 2):
    """Extracts all the points in phase space of the ball from 
    the files in the directory ( a season).  
    
    INPUT
    directory_name  name of the directory holding the json files
    order           Derivatives of the position to include in phase space. If
                    1, includes velovities, if 2 also includes accelerations, etc.
    
    OUTPUT
    """
    file_names = os.listdir(directory_name)
    coordinates = []
    for file_name in tqdm.tqdm(file_names):
        game_data = ball_data_game_extractor(directory_name + file_name,order)
        #gets rid of any trajectories with nans in them. These are due 
        #to the clock not running
        #game_data = map(lambda x,y:x[y],game_data,map(lambda x:sum(np.isnan(x).T)==0,game_data))
        if len(game_data) > order:
            for point in game_data:
                coordinates.append(point)
    return np.array(coordinates).T

def _validate_point(point):
    return (point[0]<=94)&(point[0]>=0)&(point[1]<=50)&(point[1]>=0)&(point[2]>0)

def ball_data_game_extractor(file_name,order = 2):
    """Loads the phase-space coordinates, (x,y,z,vx,vy,vz,...), for all 
    the frames of a game.
    
    INPUT
    file_name       The location of the file being read
    order           The number of time derivatives to append
    
    OUTPUT
    coordinates     An array of arrays. The top layer is the contiguous trajectories.
                    The next layer is the array of phase space coordinates of the ball 
                    along that contiguous trajectory. 
    """
    coordinates = []
    #this is the prjector that picks out the time stamp and spatial coordinates of
    # the ball from the moment data
    coordinate_projection = lambda x:[x[2],x[5][0][2],x[5][0][3],x[5][0][4]]
    try:
        #Check to make sure that the file is a json file
        if file_name[-4:] == 'json':
            #Data loading shenanigans
            json_file = open(file_name,'r')
            json_str = json_file.read().split('\n')
            #This loop goes over all the contiguous trajectories in the game
            for contiguous_play in range(len(json_str)-1):
                try:
                    moments = json.loads(json_str[contiguous_play])['moments']
                    if len(moments)>order:
                        #this is where the data is projected into the coordinate list
                        xyz = np.array(list(map(coordinate_projection,moments)))
                        #adds the velocity, acceleration, etc
                        if order > 0:
                            xyz = np.concatenate((xyz,time_delta_calc(xyz,order)),1)
                        for point in xyz.T[1:].T:
                            if _validate_point(point):
                                coordinates.append(point)
                except ValueError:
                    pass
    except IndexError:
        pass
    return np.array(coordinates)
    
def time_delta_calc(contiguous_trajectory,order = 2):
    """Computes the time derivatives of a contiguous trajectory.
    
    INPUT
    contiguous_trajectory       An array of space-time cordinates
    order           The order up to which the time derivatives are calculated. If 
                    order=1, the velocity is returned. If order=2, the velocity
                    and acceleration are returned. etc.
    
    OUTPUT
    velocity        An array of time derivatives. If order = 1, this is (vx,vy,vz).
                    If order = 2, (vx,vy,vz,ax,ay,az), etc.
    """
    eps = np.finfo(float).eps
    # take the difference in coordinates
    delta_coordinates = np.diff(contiguous_trajectory,1,0)
    # find the velocity by dividing change in spatial coordinates by change in time
    # the eps is added to prevent warnings about dividing by 0.
    velocity = map(lambda x:x[1:]/(x[0]+eps),delta_coordinates)
    # pad the velocity array with initial and final values and take the average. 
    # This is equivalent to a linear interpolation between velocities. It's done to
    # make the velocity array the same size as the coordinate array 
    velocity = np.pad(velocity,((1,1),(0,0)),'edge')
    velocity = .5*(velocity[1:,:]+velocity[:-1,:])
    #very large velocities are due to eps, and are nulled
    velocity[np.abs(velocity)>10**10]=np.nan
    if order == 1:
        return velocity
    else:
        return np.concatenate((velocity,
                               time_delta_calc(
                                   np.concatenate(
                                       (np.array([contiguous_trajectory[:,0]]).T,velocity),1),
                               order-1)),1)

#---------------------------------------------------SEASON ANALYSIS FUNCTIONS

def get_KE(coordinates):
    return np.sum(coordinates.T[3:7].T**2,axis = 1)



    