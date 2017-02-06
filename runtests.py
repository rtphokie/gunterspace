'''
Created on Feb 4, 2017

@author: trice
'''
import unittest
from gunterspace import Launches
from pprint import pprint

class Test(unittest.TestCase):


    def testName(self):
        uut = Launches()
#         for launchid in uut.launches.keys():
#             print uut.db.add_launch_row(uut.launches[launchid])

        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()