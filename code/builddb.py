import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath

import psycopg2
import os
import sys
import tqdm
from subprocess import call
import pyBall
from pyBall import ball_data_load, db_size
import numpy as np
from iter_file import iter_file

"""
builds database of game state.
"""

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
con = None
try:
    con = psycopg2.connect("host='localhost' dbname='nba_tracking' port='5432'")
    cur = con.cursor()
    cur.execute("CREATE TABLE coordinates(row BIGSERIAL PRIMARY KEY, x FLOAT NOT NULL DEFAULT 0, y FLOAT NOT NULL DEFAULT 0, z FLOAT DEFAULT NULL, vx FLOAT NOT NULL DEFAULT 0, vy FLOAT NOT NULL DEFAULT 0, vz FLOAT DEFAULT NULL );")
    con.commit()
except psycopg2.DatabaseError as e:
    if con:
        con.rollback()
    print('Error %s' % e)
    #sys.exit(1)

### populate table
try:
    coordinates = []
    file_names = os.listdir(dataPath)
    cur = con.cursor()
    for i, file_name in enumerate(tqdm.tqdm(file_names)):
        if file_name[0:7] != "nbagame": continue
        print(file_name )
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

finally:
    if con:
        con.close()

### test queries
if True:
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
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)
    coordinates[0]
    print(db_size())

