import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath

#---------------------------------------------------LIBRARIES
import json
import numpy as np
import pandas as pd
import os
import tqdm
import matplotlib
import gzip
import psycopg2
from functools import partial
import datetime

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
        game_data = ball_data_load(jsonstr_from_filename(directory_name + file_name))
        if len(game_data) > 0:
            for point in game_data:
                coordinates.append(point)
    return np.array(coordinates)

def jsonstr_from_filename(file_name):
    """
    Check to make sure that the file is a json file
    returns False if the file doesn't exist or isn't a large json file
    otherwise returns a large string ready to be processed by json.loads()
    """
    json_strs = False
    if _check_url(file_name):
        json_strs = _get_jsonurl_str(file_name) ### this can also pass False
    elif _check_json(file_name):
        json_strs = _get_json_str(file_name)
    elif _check_jsongz(file_name):
        json_strs = _get_json_str(file_name, gzipped=True)
    if json_strs and len(json_strs) <= 1:
        json_strs = False
    return(json_strs)

def all_position_data_load(json_strs, carefully=True):
    """Loads the phase-space coordinates, (gameid, playerid,t,x,y,z), for all 
    the frames of a game, for all entities (players and ball).
    
    INPUT
    json_strs       The location of the file being read
    
    OUTPUT
    coordinates     An array of arrays. The top layer is the contiguous trajectories.
                    The next layer is the array of phase space coordinates of the ball 
                    along that contiguous trajectory. 
    """
    #this is the prjector that picks out the time stamp and spatial coordinates of
    # the ball from the moment data
    
    # first get game-scale descriptives for later use
    event1 = json.loads(json_strs[0])
    gamedata = {}
    gamedata['gamedate'] = datetime.datetime.strptime(event1["gamedate"], "%Y-%m-%d")
    gamedata['gameid'] = event1['gameid']
    gamedata['home'] = event1['home']['abbreviation']
    gamedata['homeid'] = event1['home']['teamid']
    gamedata['visitor'] = event1['visitor']['abbreviation']
    gamedata['visitorid'] = event1['visitor']['teamid']
    # main coordinates object, enforcing uniquness
    headers = ("teamid", "playerid", "t", "x", "y", "z")
    coordinates = pd.DataFrame(columns=headers)
    #This loop goes over all the contiguous trajectories in the game
    t_end_last = 720
    p_last = 1
    for nevent, json_str in enumerate(json_strs):
        ###vvv no data in this event
        if len(json_str) == 0: continue
        trajectories, _t_end_last, _p_last = process_event(json_str, nevent, headers, gamedata['gameid'], t_end_last, p_last)
        if trajectories is None: 
            continue
        else:
            t_end_last, p_last = _t_end_last, _p_last
        ### surviving all of the above, add event to the output game record
        coordinates = coordinates.append( trajectories  )
        try:
            ### Sanity check that no events record the same entities in the same games at the same times 
            ###    (and no events in coordinates twwice)
            coordinates.set_index(['playerid', 'eventid', 't'], verify_integrity=True)
        except ValueError as err:
            print("PROBLEM LNM<DFJKH: Same eventid in multiple events? file %s, incl event %s" % (gamedata['gameid'], nevent))
            print(err)
            raise
    if coordinates.duplicated(subset=['t', 'playerid']).any():
        if coordinates.duplicated(subset=['t','playerid','x','y','z']).any():
            #print("PROBLEM :LDFJKS:HJ: Found and caught eliminable duplicates, entity %d" % (ientity))
            coordinates = coordinates.drop_duplicates(subset=['playerid', 't'])
        else:
            print("PROBLEM YKFHJD: TOUGH SPOT Found ultimately recoverable but mildy worrisome and hopefully nonexistent duplicates in file %s, between events (or coordinates is empty? %d), dup count: %d" % (gamedata['gameid'], coordinates.shape[0], sum(coordinates.duplicated(subset=['t', 'playerid']))))
            coordinates = coordinates.drop_duplicates(subset=['gameid', 't','playerid'])
    ### make proper key
    coordinates = coordinates.assign(gameid = gamedata["gameid"])
    try:
        coordinates.set_index(['gameid', 'playerid', 't'], verify_integrity=True)
    except:
        print("PROBLEM :LKFJD: intractable duplicates file %s (or coordinates is empty? %d)" % (gamedata['gameid'], coordinates.shape[0]))
        if coordinates.shape[0] > 0:
            raise
            #pass
    ### now add misc columns
    #TODO assign binary in/out of possession flag to each row
    coordinates = coordinates.assign(gamedate = gamedata["gamedate"])
    coordinates = coordinates.assign(period = (coordinates[['t']] // 720) + 1)
    ### add discretizations
    if True:
        bl = coordinates
        bl = bl.assign(bit2 = np.where(bl.x <= 94/2, 0, 1))
        bl = bl.assign(bit3 = np.where(bl.y <= 50/2, 0, 1))
        ### calculate ball within or beyond 3 point line.
        ###  http://www.nba.com/analysis/rules_1.html
        ###  33pt line is a 23.75 foot part-circle on top of a 22x14.2 foot rectangle:
        ###       22*tan(acos(22/23.75)) + 5.25 = 14.2
        ###    first symmetrize down to quarter court
        bl = bl.assign(xsym = np.where(bl.x <= 94/2, bl.x, (94-bl.x)))
        bl = bl.assign(ysym = np.where(bl.y <= 50/2, bl.y, (50-bl.y)))
        ###    then define state within arc-on-box 3pt line contour
        bl = bl.assign(bit1 = np.where((bl.ysym > 3) & (bl.xsym <= 14.2), 1, 0))
        bl = bl.assign(bit1 = np.where((bl.ysym > 3) & (bl.xsym > 14.2) & (bl.xsym < ((23.75**2 - (bl.ysym - 25)**2)**0.5 + 5.25)), 1, bl.bit1))
        ###    construct eight court states;
        bl = bl.assign(state = 2**0*bl.bit1 + 2**1*bl.bit2 + 2**2*bl.bit3)
        coordinates = coordinates.assign(state = bl.state)
    return(coordinates)

def process_event(json_str, nevent, headers, gameid, t_end_last, p_last):
    moments = json.loads(json_str)['moments']
    #this is where the data is projected into the coordinate list
    ### repeat this for each entity on the court
    ### filter out invalid moments
    moments_valid = list(filter(lambda x:
            (len(x[5])==11) and #those without 11 entities on the court (by checking for for 11 event lists)
            (x[2] != 720) and #those with t=720 
                                #(very very beginning of period, known to be buggy)
            ((x[2] < t_end_last) or (x[0] != p_last))
                                #those with event overlaps 
                                #(in the same period, and beginning before the last event ended)
        , moments))
    #moments_valid =  moments
    ###vvv no data in event after filtering
    if len(moments_valid) < 2: return((None,None,None))
    ### reset the overlap tracker (assumes events are temporally ordered)
    t_end_last = min([x[2] for x in moments_valid]) ## time that this event ended
    p_last = moments[0][0] ### period of this event 
                            ### (assumes that all events are within period)
                            ### (don't sweat this using moments instead of moments_valid)
    trajectories = []
    for ientity in range(11):
        try:
            trajectory = process_entity(moments_valid, headers, ientity)
            if trajectory is None: break
            trajectories.append( trajectory )
        except:
            print("%s %d %d"%(gameid, nevent, ientity))
            raise
    if True: ### a few filters with criteria for skipping an event
        if len(trajectories) == 0 : return((None,None,None)) #print("PASS SDJFGR: Skips for no data")
        if any(i is None for i in trajectories): return((None,None,None)) #print("PASS JFGDF: Skips for bad duplicates")
        ### missing player check
        ents = set()
        _ = [ents.update(df.playerid) for df in trajectories]
        if len(ents) != 11:
            #print("RARE PROBLEM HSDGJF: missing players: %s %d %d"%(gameid, nevent, len(ents)))
            return((None,None,None))
    ### now combine entity objects into event-scale team-scale data
    trajectories = pd.concat( trajectories, ignore_index=True)
    trajectories = trajectories.assign(eventid = nevent)
    ### now check for index violations, an issue caused by entities crossing traj objects, 
    ###    and wierd collisions like the ball being an amny places at once
    if trajectories.duplicated(subset=['t', 'playerid']).any():
        if trajectories.duplicated().any():
            print("PROBLEM IOAJSDJ: Found and caught eliminable duplicates, entity %d" % (ientity))
            trajectories = trajectories.drop_duplicates(subset=['playerid', 't'])
        else:
            return((None,None,None))
    ### now eliminate time steps that lack 11 entities
    timesBad = trajectories.groupby(['t']).count().query('playerid != 11').index
    trajectories = trajectories[~trajectories.t.isin(timesBad)]
    try:
        trajectories.set_index(['playerid', 'eventid', 't'], verify_integrity=True)
    except:
        print("PROBLEM TKJFLL: Found duplicates unrecoverably, file %s, event %s" % (gameid, nevent))
        raise
    return(( trajectories, t_end_last, p_last ))

def process_entity(moments_valid, headers, ientity):
    ### set up the coordinate project for this entity
    ### at this instant, time becomes something that counts up instead of down
    mapfunc = partial(_coordinate_projection_entity,ientity=ientity)
    ### produce 2D array of all states over time
    trajectory = np.array(list(map(mapfunc,moments_valid)))
    ### remove all instances of time freezing
    trajectory = _clean_time(trajectory.T, itime=2).T
    ### filter out invalid entity states
    trajectory = np.array(list(filter(lambda x: _validate_position(x[3:6], ball=(x[0] == -1)), trajectory)))
    ### don't just filter out completely empty sequences, but also those that leave just one observation
    if len(trajectory) < 2: return(None)
    trajectory = pd.DataFrame(data=trajectory, columns=headers)
    return(trajectory)

def ball_data_load(json_strs):
    coordinates = all_position_data_load( json_strs) ### load full object with all entities
    coordinates = coordinates.query("playerid == -1") ### pick out ball entitities
    coord = _add_velocity( np.array(coordinates.loc[:,("t","x","y","z")].T  )).T ### add velocities
    coord = coord[:,1:7] ### cut timestamps (move this line down 1 if i decdie I want them)
    coord = np.array(list(filter(lambda x: _validate_velocity(x), coord)))   ### filter out invalid velocities
    return(coord)

def ball_data_load_old(file_name):
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
        else:
            json_strs = _get_json_str(file_name)
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

def court_phase_space_generate_db(n="full"):
    if False:
        coordinates = np.array([])
        try:
            con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
            cur = con.cursor()
            if n == "full":
                cur.execute("SELECT COUNT(*) FROM gamestate")
                n = cur.fetchall()
            #https://stackoverflow.com/questions/32356330/how-do-i-generate-a-random-sample-of-groups-including-all-people-in-the-group
            query = """
            SELECT gs_out.game, gs_out.team, gs_out.pid, gs_out.t, gs_out.state FROM gamestate AS gs_out
                JOIN (
                    SELECT game, t FROM gamestate GROUP BY game, t ORDER BY RANDOM() LIMIT 4
                ) AS gs_groupkey ON (gs_out.game = gs_groupkey.game AND gs_out.t = gs_groupkey.t)
            ORDER BY gs_out.game, gs_out.t;
                """
                ### with replacement (and speed up tricks): https://www.periscopedata.com/blog/how-to-sample-rows-in-sql-273x-faster
            cur.execute("SELECT game, pid, t, state FROM gamestate LIMIT %s"%(n,))
            #https://pythonspot.com/python-database-postgresql/
            #while True:
                #row = cur.fetchone()
                #if row == None: break
            coordinates = np.array(cur.fetchall())
        except psycopg2.DatabaseError as e:
            if con:
                con.rollback()
            print('Error %s' % e)
        coordinates = _add_velocity( coordinates.T ).T ### add velocities
        coordinates = coordinates[:,1:7] ### cut timestamps (move this line down 1 if i decdie I want them)
        coordinates = np.array(list(filter(lambda x: _validate_velocity(x), coordinates)))   ### filter out invalid velocities
        return( coordinates )

def build_distribution(n="full", id_max=False):
    ### init output dist
    dist = np.zeros([4,4,4,4,4])
    ### get dbsize
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        raise e
    if not id_max:
        cur.execute("SELECT MAX(row) FROM gamestate")
        id_max = cur.fetchone()[0]
    ### init query
    #n = '' if n == "full" else 'LIMIT {}'.format(n)
    n = id_max if n == "full" else n
    #https://stackoverflow.com/questions/32356330/how-do-i-generate-a-random-sample-of-groups-including-all-people-in-the-group
    query = """
    SELECT gout.game, gout.teamid, gout.pid, gout.t * 100 AS t, gout.state FROM gamestate AS gout
        JOIN (
            SELECT game, t, obs FROM (
                -- vvv this randomly select game times, so I can pull all 11 obs of random moments
                SELECT MIN(row) AS therow, game, t, COUNT(*) AS obs FROM gamestate GROUP BY game, t
            )
            AS f
            WHERE
            therow IN ( -- https://www.periscopedata.com/blog/how-to-sample-rows-in-sql-273x-faster
                SELECT ROUND(RANDOM() * {})::INTEGER AS therow
                FROM GENERATE_SERIES(1, {})
                -- GROUP BY therow -- Discard or keep duplicates (commented means keep dublicates---saomple with replacement)
            )
            GROUP BY game, t, obs LIMIT {}
        --- match core table to randommly selected rows
        ---     the 11 is controlling  for a few bad entries, edge cases of double inputs
        ) AS gkey ON (gout.game = gkey.game AND gout.t = gkey.t and gkey.obs = 11)
    ORDER BY gout.game, gout.t, gout.teamid, gout.pid;
        """.format(id_max, id_max, n)
    ### TODO fix build/load error causing obs = 11 failures (costly to catch here)
    ### connect to db and execute, in safety
    ### SLOW
    cur.execute( query )
    ### with cursor, build distribution in a memory efficient manner
    states = np.array([])
    while True:
        prev = states
        ### WARNING: change dtype below to float if ever using x,y,or z
        states = np.array(cur.fetchmany(11), dtype=int).transpose() ### note *100 in query so that t's dont get truncated on conversion to int
        if not states.size: break ### end of query hits
        ### sanity and hygeine tests, thorough
        try:
            assert len(np.unique(states[3])) == 1, "states are coming from different points in time {}".format(np.unique(states[3]))
            assert len(np.unique(states[0])) == 1, " ... or different games: {}".format(np.unique(states[0]))
            assert len(np.unique(states[1])) == 3, "... or more than three teamids: {}".format(np.unique(states[1])) #(t1, t2, and the ball in its own league)
            assert len(np.unique(states[2])) == len(states[2]), "... fewer than 11 entities: {} {} {}".format(len(np.unique(states[2])), len(states[2]), np.unique(states[2]))
        except AssertionError as e:
            print(e)
            print(prev)
            print(states)
            print("it gets much worse after this")
            raise e
        t1 = states[4][1:6] // 2  ### the // 2 is to convert 8 states down to (tractable) 4
        t2 = states[4][6:11] // 2
        dist[tuple(t1)] += 1
        dist[tuple(t2)] += 1
    return( dist )

def build_distribution_alt(n="full"):
    ### init output dist
    dist = np.zeros([4,4,4,4,4])
    ### init query
    n = '' if n == "full" else 'LIMIT {}'.format(n)
    #https://stackoverflow.com/questions/32356330/how-do-i-generate-a-random-sample-of-groups-including-all-people-in-the-group
    query = """
    SELECT gout.game, gout.teamid, gout.pid, gout.t * 100, gout.state FROM gamestate AS gout
        JOIN (
            SELECT game, t, obs FROM (
                SELECT game, t, count(*) AS obs FROM gamestate GROUP BY game, t -- this randomly select game times, so I can pull all 11 obs of random moments
            ) AS f WHERE obs = 11 GROUP BY game, t, obs -- obs = 11 is controlling  for a few bad entries, edge cases of double inputs
            ORDER BY RANDOM()  {}
        ) AS gkey ON (gout.game = gkey.game AND gout.t = gkey.t ) --- match core table to randommly selected rows
    ORDER BY gout.game, gout.t, gout.teamid, gout.pid;
        """.format(n)
    ### TODO with replacement (and speed up tricks): https://www.periscopedata.com/blog/how-to-sample-rows-in-sql-273x-faster
    ### TODO fix bug causing obs = 11 failures
    ### connect to db and execute, in safety
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute( query )
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
    ### with cursor, build distribution in a memory efficient manner
    states = np.array([])
    while True:
        prev = states
        ### WARNING: change dtype below to float if ever using x,y,or z
        states = np.array(cur.fetchmany(11), dtype=int).transpose() ### note *100 in query so that t's dont get truncated on conversion to int
        if not states.size: break ### end of query hits
        ### sanity and hygeine tests, thorough
        try:
            assert len(np.unique(states[3])) == 1, "states are coming from different points in time {}".format(np.unique(states[3]))
            assert len(np.unique(states[0])) == 1, " ... or different games: {}".format(np.unique(states[0]))
            assert len(np.unique(states[1])) == 3, "... or more than three teamids: {}".format(np.unique(states[1])) #(t1, t2, and the ball in its own league)
            assert len(np.unique(states[2])) == len(states[2]), "... fewer than 11 entities: {} {} {}".format(len(np.unique(states[2])), len(states[2]), np.unique(states[2]))
        except AssertionError as e:
            print(e)
            print(prev)
            print(states)
            print("it gets much worse after this")
            raise e
        t1 = states[4][1:6] // 2  ### the // 2 is to convert 8 states down to (tractable) 4
        t2 = states[4][6:11] // 2
        dist[tuple(t1)] += 1
        dist[tuple(t2)] += 1
    return( dist )

if False:
    if True:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT MAX(row) FROM gamestate")
        id_max = cur.fetchone()[0]
    dist = build_distribution(10, id_max)
    assert dist.sum() == 20
    distfull = build_distribution(id_max=id_max)
    print(distfull.sum())

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
        cur.execute("SELECT t, x, y, z FROM gamestate WHERE pid = -1 LIMIT %s"%(n,))
        #https://pythonspot.com/python-database-postgresql/
        #while True:
            #row = cur.fetchone()
            #if row == None: break
        coordinates = np.array(cur.fetchall())
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    coordinates = _add_velocity( coordinates.T ).T ### add velocities
    coordinates = coordinates[:,1:7] ### cut timestamps (move this line down 1 if i decdie I want them)
    coordinates = np.array(list(filter(lambda x: _validate_velocity(x), coordinates)))   ### filter out invalid velocities
    return( coordinates )

def ball_phase_space_generate_db_old(n):
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
    out = False
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM gamestate")
        out = cur.fetchone()[0]
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
    return( out )


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

def _check_url(file_name):
    #checks to see if the root of a file is http
    return file_name[:7]=='http://'

def _get_json_str(file_name, gzipped=False):
    if not gzipped:
        json_file = open(file_name,'r')
    else:
        json_file = gzip.open(file_name,'rt')
    return json_file.read().split('\n')

def _get_jsonurl_str(file_name, gzipped=True):
    ### https://stackoverflow.com/questions/9419162/python-download-returned-zip-file-from-url/14260592jj
    import requests, io
    r = requests.get(file_name)
    if r.ok:
        if not gzipped:
            json_file = open(io.BytesIO(r.content),'rt')
        else:
            json_file = gzip.open(io.BytesIO(r.content),'rt')
        json_file = json_file.read().split('\n')
    else:
        json_file = False
    return(json_file)

def _clean_time(trajectory, itime=0):
    """ given a 2d array and the index of its time column,
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

if __name__ == '__main__' and False:
    #np.set_printoptions(formatter={'float_kind':'{:f}'.format}, floatmode="maxprec")
    np.set_printoptions(formatter={'float_kind':'{:f}'.format})
    file_name = dataPath + "nbagame0021400377.json.gz"
    json_strs = _get_json_str(file_name, gzipped=True)
    coordinates = all_position_data_load( jsonstr_from_filename(file_name ))
    coordinates_old = ball_data_load_old( file_name )
    if True:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT gamedate, game, event, teamid, pid, t, x, y, z, state FROM gamestate")
        coorddb = pd.DataFrame(cur.fetchall())
    #coordinates_old = ball_data_load( file_name )
    print(coordinates_old[0:5])
    print(coordinates[0:5])
    print(coorddb[0:5])
    print()
    print()
    print(type(coordinates_old[0:5]))
    print(type(coordinates[0:5]))

