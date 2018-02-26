import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath

import os
import sys
from subprocess import call
import numpy as np
import pandas as pd
import tqdm
import psycopg2

import pyBall
from pyBall import ball_data_load, db_size, all_position_data_load
from iter_file import iter_file

"""
builds database of game state.
"""

def hard_create_db():
    ### create dataBase
    dbfile = open(codePath+"zzzhelp_builddb.sql", "w")
    dbfile.write("""
    DROP DATABASE IF EXISTS nba_tracking;
    CREATE DATABASE nba_tracking;
    --\c nba_tracking
    """)
    dbfile.close()
    call([ psqlPath+"psql", "-f", codePath+"zzzhelp_builddb.sql"])
    call([ "rm", codePath+"zzzhelp_builddb.sql"])

### create table
def create_entity_table(con):
    try:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS gamestate")
        cur.execute("CREATE TABLE gamestate(row SERIAL PRIMARY KEY, gamedate TIMESTAMP WITHOUT TIME ZONE, game VARCHAR(10) NOT NULL DEFAULT '', event INTEGER NOT NULL DEFAULT 0, teamid INTEGER NOT NULL DEFAULT 0, pid INTEGER NOT NULL DEFAULT 0, t FLOAT NOT NULL DEFAULT 0, x FLOAT NOT NULL DEFAULT 0, y FLOAT NOT NULL DEFAULT 0, z FLOAT DEFAULT NULL, event SMALLINT NOT NULL DEFAULT 0);")
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
        #sys.exit(1)

def populate_entity_table_full(con, ballonly=False, dryrun=False):
    ### populate table
    file_names = os.listdir(dataPath)
    file_names_valid = []
    cur = con.cursor()
    for i, file_name in enumerate(file_names):
        #if i < 425: continue ### <<< temp code for continuing job mid-chug
        ### this prevents DS_Store files from crashing the thing, but this may 
        ###  interfere with processessing other seasons in different file/naming formats
        if file_name[0:7] != "nbagame": continue
        if os.stat(dataPath + file_name).st_size < 50: continue  ### empyt zip files
        file_names_valid.append(file_name)
    try:
        for i, file_name in enumerate(tqdm.tqdm(file_names_valid)):
            populate_entity_table(dataPath + file_name, cur, ballonly=ballonly)
            if dryrun:
                cur.execute("CREATE UNIQUE INDEX ON gamestate (game, pid, t)")
                con.rollback()
        if not dryrun:
            con.commit()
            cur.execute("CREATE UNIQUE INDEX ON gamestate (game, pid, t)")
            con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print( "Break at %s" % file_name )
        print( 'Error %s' % e )
        raise

def populate_entity_table(file_name, cur, ballonly=False):
    if not ballonly:
        coordinates = all_position_data_load(file_name)
    else:
        coordinates = ball_data_load(file_name)
        print(coordinates.columns)
    ### in case there is cod etha tisn't performing discretization
    if 'state' not in coordinates.columns:
        coordinates = coordinates.assign(state=0)
    #https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
    ### add games/files one at a time
    #print(coordinates.shape)
    #print([type(x) for x in coordinates.iloc[0,:]])
    ### FROM Index(['eventid', 'playerid', 't', 'teamid', 'x', 'y', 'z', 'gameid', 'gamedate', 'period'],
    ### TO schema:
    ###  row SERIAL PRIMARY KEY, 
    ###  gamedate TIMESTAMP WITHOUT TIME ZONE, 
    ###  game VARCHAR(10) NOT NULL DEFAULT '', 
    ###  event INTEGER NOT NULL DEFAULT 0, 
    ###  teamid INTEGER NOT NULL DEFAULT 0, 
    ###  pid INTEGER NOT NULL DEFAULT 0, 
    ###  t FLOAT NOT NULL DEFAULT 0, 
    ###  x FLOAT NOT NULL DEFAULT 0, 
    ###  y FLOAT NOT NULL DEFAULT 0, 
    ###  z FLOAT DEFAULT NULL,
    ### event SMALLINT NOT NULL DEFAULT 0);
    #current order: ['teamid', 'playerid', 'event', 't', 'x', 'y', 'z', 'game', 'gamedate', 'period', 'state']
    outs = ("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(x['gamedate'], x['gameid'], int(x['eventid']), int(x['teamid']), int(x['playerid']), x['t'], x['x'], x['y'], x['z'], x['state'])
                                for x in coordinates.to_records())
    f = iter_file.IteratorFile(outs)
    cur.copy_from(f, 'gamestate', columns=('gamedate', 'game', 'event', 'teamid', 'pid', 't', 'x', 'y', 'z', 'state'))

def entity_db(dryrun=False):
    con = None
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        create_entity_table(con)
        #populate_entity_table_full(con)
        populate_entity_table_full(con, dryrun=dryrun)
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    finally:
        if con:
            con.close()


### create table
def create_ball_table(con):
    try:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS coordinates")
        cur.execute("CREATE TABLE coordinates(row BIGSERIAL PRIMARY KEY, x FLOAT NOT NULL DEFAULT 0, y FLOAT NOT NULL DEFAULT 0, z FLOAT DEFAULT NULL, vx FLOAT NOT NULL DEFAULT 0, vy FLOAT NOT NULL DEFAULT 0, vz FLOAT DEFAULT NULL );")
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
        #sys.exit(1)

def populate_ball_table(con):
    ### populate table
    try:
        coordinates = []
        file_names = os.listdir(dataPath)
        cur = con.cursor()
        for i, file_name in enumerate(tqdm.tqdm(file_names)):
            ### this prevents DS_Store files from crashing the thing, but this may 
            ###  interfere with processessing other seasons in different file/naming formats
            if file_name[0:7] != "nbagame": continue
            if os.stat(dataPath + file_name).st_size < 50: continue  ### empyt zip files
            game_data = ball_data_load(dataPath + file_name)
            if len(game_data) > 0:
                for point in game_data:
                    #cur.execute("INSERT INTO coordinates (x,y,z,vx,vy,vz) VALUES (%s, %s, %s, %s, %s, %s)"%tuple(point))
                    coordinates.append(point)
            #https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
            ### add games/files one at a time
            f = iter_file.IteratorFile(("{}\t{}\t{}\t{}\t{}\t{}".format(x[0], x[1], x[2], x[3], x[4], x[5]) for x in coordinates))
            cur.copy_from(f, 'coordinates', columns=('x', 'y', 'z', 'vx', 'vy', 'vz'))
            coordinates = []
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)

def ball_db():
    con = None
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        create_ball_table(con)
        populate_ball_table(con)
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    finally:
        if con:
            con.close()

hard_create_db()
entity_db(dryrun=True)
#ball_db()

### test queries
if __name__ == "__main__":
    coordinates = np.array([])
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        cur = con.cursor()
        cur.execute("SELECT x, y, z, vz, vy, vz FROM coordinates LIMIT 1000")
        #https://pythonspot.com/python-database-postgresql/
        #while True:
            #row = cur.fetchone()
            #if row == None: break
        coordinates = np.array(cur.fetchall())
        if coordinates:
            print(coordinates[0])
            print(coordinates.shape)
            print()
        cur.execute("SELECT gamedate, game, event, teamid, pid, t, x, y, z FROM gamestate LIMIT 1000")
        coordinates = pd.DataFrame(cur.fetchall())
        #cur.execute("CREATE UNIQUE INDEX ON gamestate (game, pid, t)")
        #cur.execute("CREATE INDEX ON gamestate (game, event, pid, t)")
        con.commit()
        #print(coordinates.iloc[0,:])
        print(coordinates.shape)
        if False:
            file_name = "nbagame0021400164.json.gz"
            file_name = "nbagame0021400044.json.gz"
            file_name = "nbagame0021400314.json.gz"
            file_name = "nbagame0021400308.json.gz"
            populate_entity_table(dataPath +  file_name, cur)
            coordinates = all_position_data_load(dataPath + file_name)
            json_strs = _get_json_str(dataPath + file_name, gzipped=True)
            headers = ("teamid", "playerid", "t", "x", "y", "z")
            coord = pd.DataFrame(columns=headers)
            ientity = 0
            nevent = 96
            nevent = 266
            moments = json.loads(json_strs[nevent])['moments']
            t_end_last = 720
            p_last = moments[0][0]
            moments_valid = list(filter(lambda x:
                    (len(x[5])==11) and #those without 11 entities on the court
                    (x[2] != 720) and #those with t=720 
                                        #(very very beginning of period, known to be buggy)
                    ((x[2] < t_end_last) or (x[0] != p_last))
                                        #those with event overlaps 
                                        #(in the same period, and beginning before the last event ended)
                , moments))

            trajectory = process_entity(moments_valid, headers, ientity)
         #cur.execute("CREATE UNIQUE INDEX ON gamestate (game, event, pid, t)")
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
    #coordinates[0]
    print(db_size())

