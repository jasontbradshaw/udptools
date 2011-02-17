#!/usr/bin/env python

import unittest
from udptools import UDPDump

class DumpTests(unittest.TestCase):
    def setUp(self):
        self.u = UDPDump()

    def tearDown(self):
        self.u.stop()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DumpTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
