#!/usr/bin/env python

import unittest
import udpdump

class DumpTests(unittest.TestCase):
    def setUp(self):
        self.r = Recorder()
        self.r.rtpplay = rtp.RTPPlayEmulator()
        self.r.rtpdump = rtp.RTPDumpEmulator()
    
    def tearDown(self):
        pass
    
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DumpTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
