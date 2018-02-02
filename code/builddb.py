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
        cur.execute("CREATE TABLE gamestate(row SERIAL PRIMARY KEY, gamedate TIMESTAMP WITHOUT TIME ZONE, gameid INTEGER NOT NULL DEFAULT 0, teamid INTEGER NOT NULL DEFAULT 0, pid INTEGER NOT NULL DEFAULT 0, period SMALLINT NOT NULL DEFAULT 0, t FLOAT NOT NULL DEFAULT 0, x FLOAT NOT NULL DEFAULT 0, y FLOAT NOT NULL DEFAULT 0, z FLOAT DEFAULT NULL);")
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
        #sys.exit(1)

def populate_entity_table(con):
    ### populate table
    try:
        file_names = os.listdir(dataPath)
        cur = con.cursor()
        for i, file_name in enumerate(tqdm.tqdm(file_names)):
            ### this prevents DS_Store files from crashing the thing, but this may 
            ###  interfere with processessing other seasons in different file/naming formats
            if file_name[0:7] != "nbagame": continue
            if os.stat(dataPath + file_name).st_size < 50: continue  ### empyt zip files
            coordinates = all_position_data_load(dataPath + file_name)
            #current order: ['teamid', 'playerid', 't', 'x', 'y', 'z', 'gameid', 'gamedate', 'period']
            #https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
            ### add games/files one at a time
            #print(list(x for x in np.array(coordinates.head())))
            f = iter_file.IteratorFile(("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(x[7], int(x[6]), int(x[0]), int(x[1]), x[2], x[3], x[4], x[5])
                                        for x in np.array(coordinates)))
            cur.copy_from(f, 'gamestate', columns=('gamedate', 'gameid', 'teamid', 'pid', 't', 'x', 'y', 'z'))
            coordinates = []
        con.commit()
        cur.execute("CREATE INDEX ON gamestate (gameid, pid, t)")
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)


def entity_db():
    con = None
    try:
        con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
        create_entity_table(con)
        populate_entity_table(con)
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
entity_db()
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
        print(coordinates[0])
        print(coordinates.shape)
        print()
        cur.execute("SELECT gamedate, gameid, teamid, pid, x, y, z FROM gamestate LIMIT 1000")
        coordinates = pd.DataFrame(cur.fetchall())
        #cur.execute("CREATE UNIQUE INDEX ON gamestate (gameid, pid, t)")
        con.commit()
        print(coordinates.iloc[0,:])
        print(coordinates.shape)
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    coordinates[0]
    print(db_size())

