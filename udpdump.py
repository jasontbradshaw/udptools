#!/usr/bin/env python

import socket
import base64
import time

def dump(dump_file, host, port, bufsize=16384):
    """
    Dump UDP traffic to a file.
    """

    # set up the socket we're capturing packets from
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, port))
    
    # dump packets to the file
    with open(dump_file, 'w') as f:
        while 1:
            data = base64.b64encode(s.recv(bufsize))
            f.write("%.5f\t%s\n" % (time.time(), data))

if __name__ == "__main__":
    import sys

    # the file to dump to
    dump_file = sys.argv[1]

    # host and port to get packets from
    host = sys.argv[2]
    port = int(sys.argv[3])
    
    # record packets
    dump(dump_file, host, port)

