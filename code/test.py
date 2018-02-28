import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath
sys.path.append(codePath) ### because of unittest wierdness

import pyBall
import json
import unittest
import numpy as np
from functools import partial

class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

class TestCoordTestData(unittest.TestCase):

    def setUp(self):
        file_name = dataPath + "nbagame0021400377.json.gz"
        json_strs = pyBall._get_json_str(file_name, gzipped=True)
        self.file_name = file_name
        self.json_strs = json_strs
        ### now make finer subobjects
        json_str = json_strs[10]
        moments = json.loads(json_str)['moments']
        self.trajectory = np.array(list(map(pyBall._coordinate_projection_ball,moments))).T

    def test_coordinates(self):
        json_strs = self.json_strs
        coordinates = []
        for json_str in json_strs:
            try:
                moments = json.loads(json_str)['moments']
                if len(moments)>5:
                    #this is where the data is projected into the coordinate list
                    trajectory = np.array(list(map(pyBall._coordinate_projection_ball,moments))).T
                    #adds the velocity, acceleration, etc
                    trajectory = pyBall._add_velocity(trajectory)
                    for point in trajectory[1:].T:
                        if pyBall._validate_point(point):
                            coordinates.append(point)
            except ValueError:
                pass
        #self.assertTrue(len(coordinates) == 173856, len(coordinates))
        self.assertTrue(len(coordinates[-1]) == 6)
        self.assertTrue(len(trajectory) == 7)
        self.assertTrue( all([ x == 6 for x in map(len, coordinates)] ))

    def test_point_load(self):
        json_strs = self.json_strs
        coordinates = []
        json_str = json_strs[10]
        moments = json.loads(json_str)['moments']
        #this is where the data is projected into the coordinate list
        trajectory = np.array(list(map(pyBall._coordinate_projection_ball,moments))).T
        #adds the velocity, acceleration, etc
        #self.assertTrue(False, "trajworking: %s; shape:%s"%(str(trajectory.T[0]), str(trajectory.T.shape)))
        trajectory = pyBall._add_velocity(trajectory)
        #for point in trajectory[1:].T:
            #if pyBall._validate_point(point):
                #coordinates.append(point)
        point = trajectory[1:].T[0]
        self.assertTrue(len(point) == 6, "moments: %d; point: %d; valid: %d"%(len(moments),len(point), pyBall._validate_point(point)))
        self.assertTrue(pyBall._validate_point(point))

    def test_point_load2(self):
        json_strs = self.json_strs
        coordinates = []
        json_str = json_strs[10]
        moments = json.loads(json_str)['moments']
        #this is where the data is projected into the coordinate list
        mapfunc = partial(pyBall._coordinate_projection_entity,ientity=0)
        trajectory = np.array(list(map(mapfunc,moments))).T
        #adds the velocity, acceleration, etc
        self.assertTrue(len(trajectory) > 0, "traj: %s"%(str(trajectory)))
        #self.assertTrue(False, "trajnotworking: %s; shape:%s"%(str(trajectory[2:6].T[0]), str(trajectory.T.shape)))
        trajectory = pyBall._add_velocity(trajectory[2:6])
        self.assertTrue(len(trajectory) > 0, "traj2: %s; shape:%s"%(str(trajectory), str(trajectory.T.shape)))
        #self.assertTrue(False, "traj2: %s; shape:%s"%(str(trajectory), str(trajectory.T.shape)))
        #for point in trajectory[1:].T:
            #if pyBall._validate_point(point):
                #coordinates.append(point)
            #print(point)
            #if _validate_point(point[3:9]):
                #coordinates.append(point[3:9])
        point = trajectory[:].T[0]
        self.assertTrue(len(point) == 7, "moments: %d; point: %d; valid: %d, point: %s"%(len(moments),len(point), pyBall._validate_point(point), str(point)))
        self.assertTrue(pyBall._validate_point(point[1:]))

    def test_ball_data_load(self):
        coordinates = pyBall.ball_data_load( self.file_name )
        #vvv former test is wrong, because of convoluaiotn-based time filtering : 
        #self.assertTrue(len(coordinates) == 173856, len(coordinates))
        #self.assertTrue(len(coordinates) == 173763, len(coordinates))
        self.assertTrue(len(coordinates[-1]) == 6)
        self.assertTrue( all([ x == 6 for x in map(len, coordinates)] ))
        ### test refactoring of ball loader
        c1 = pyBall.ball_data_load( self.file_name )
        c2 = pyBall.ball_data_load_old( self.file_name )
        self.assertTrue( np.array_equal( c1[0:5], c2[0:5]), "%s != %s" % (str(c1.shape) , str(c2.shape) ) )

    def test_all_position_data_load(self):
        coordinates = pyBall.all_position_data_load( pyBall.jsonstr_from_filename(self.file_name))
        #self.assertTrue(len(coordinates) == 201747, len(coordinates))

    def test_clean_time(self):
        trajectory1 = pyBall._clean_time(self.trajectory)
        trajectory1 = pyBall._add_velocity(trajectory1)
        trajectory2 = pyBall._add_velocity_old(self.trajectory)
        self.assertTrue( trajectory1.shape[0] == trajectory2.shape[0], "%s != %s" % (trajectory1.shape , trajectory2.shape ) )
        self.assertTrue( trajectory1.shape[1] == trajectory2.shape[1], "%s != %s" % (trajectory1.shape , trajectory2.shape ) )
        self.assertTrue( np.array_equal(trajectory1 , trajectory2), "%s != %s" % (str(trajectory1[0]) , str(trajectory2[0]) ) )

    def test_db_draws(self):
        c1 = pyBall.ball_phase_space_generate_db(1000)
        c2 = pyBall.ball_phase_space_generate_db_old(1000)
        ### deprecated if/because ball coordinates not loaded anymore
        self.assertTrue( np.array_equal( c1, c2), "%s != %s" % (str(c1.shape) , str(c2.shape) ) )


if __name__ == '__main__':
    unittest.main()
