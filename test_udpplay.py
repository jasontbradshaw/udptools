#!/usr/bin/env python

import unittest
from udptools import UDPPlay

class PlayTests(unittest.TestCase):
    def setUp(self):
        self.u = UDPPlay()

    def tearDown(self):
        self.u.stop()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(PlayTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
