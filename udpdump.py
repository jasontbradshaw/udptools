#!/usr/bin/env python

import sys
import time

from udptools import UDPDump

# the file to dump to
dump_file = sys.argv[1]

# host and port to get packets from
host = sys.argv[2]
port = int(sys.argv[3])

# record packets
udpdump = UDPDump()
udpdump.dump(dump_file, host, port)

try:
    while udpdump.is_running():
        time.sleep(0.1)
except KeyboardInterrupt:
    udpdump.stop()
