import sys
sys.path.extend((".",".."))
from local_settings import * ### for codePath dataPath psqlPath
sys.path.append(codePath) ### because of unittest wierdness

import pyBall
import json
import unittest
import numpy as np

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
        self.file_name = "/Users/sfrey/Desktop/projecto/research_projects/nba_tracking/sampledata/nbagame0021400377.json.gz"
        json_strs = pyBall._get_json_str(self.file_name, gzipped=True)
        self.json_strs = json_strs

    def test_coordinates(self):
        coordinates = []
        for json_str in self.json_strs:
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
        self.assertTrue(len(coordinates) == 173317)
        self.assertTrue(len(coordinates[-1]) == 6)
        self.assertTrue(len(trajectory) == 7)
        self.assertTrue( all([ x == 6 for x in map(len, coordinates)] ))

    def test_ball_data_load(self):
        coordinates = pyBall.ball_data_load( self.file_name )
        self.assertTrue(len(coordinates) == 173317)
        self.assertTrue(len(coordinates[-1]) == 6)
        self.assertTrue( all([ x == 6 for x in map(len, coordinates)] ))

    def test_all_position_data_load(self):
        coordinates = pyBall.all_position_data_load( self.file_name )
        self.assertTrue(len(coordinates) == 201747)

if __name__ == '__main__':
    unittest.main()
