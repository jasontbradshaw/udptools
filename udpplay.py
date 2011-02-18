#!/usr/bin/env python

import sys
import optparse
import time

from udptools import UDPPlay

# parse optional arguments first
parser = optparse.OptionParser()
parser.add_option("-b", "--begin", dest="begin_time", default=0.0,
        type="float",
        help="Begin playback at this time instead of the beginning.")
parser.add_option("-e", "--end", dest="end_time", default=None,
        type="float",
        help="End playback after this time instead of at the end.")
options, args = parser.parse_args()

# require the dump file
dump_file = args[0]

# also require the host and port to dump it to
host = args[1]
port = int(args[2])

# play the file
udpplay = UDPPlay()
udpplay.play(dump_file, host, port, begin_time=options.begin_time,
        end_time=options.end_time)

try:
    while udpplay.is_running():
        time.sleep(0.1)
except KeyboardInterrupt:
    udpplay.stop()
