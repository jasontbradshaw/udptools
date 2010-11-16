#!/usr/bin/env python

import socket
import base64
import time

def dump(dump_file, host, port, bufsize=16384, file_format="%.10f\t%s\n"):
    """
    Dump UDP traffic to a file.
    """

    # set up the socket we're capturing packets from
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, port))
    
    # dump packets to the file
    with open(dump_file, 'w') as f:
        
        # time codes are relative to the first packet received, which has time
        # 0.0.  we set it after recv so any delay before traffic doesn't show up
        # in the start time.
        start_time = None
        while 1:
            data = base64.b64encode(s.recv(bufsize))
            recv_time = time.time()
            
            # mark first received packet time and set time in file to '0.0'
            if start_time is None:
                start_time = recv_time
                f.write(file_format % (0.0, data))
                continue
            
            # write time elapsed from start plus data
            f.write(file_format % (recv_time - start_time, data))

if __name__ == "__main__":
    import sys

    # the file to dump to
    dump_file = sys.argv[1]

    # host and port to get packets from
    host = sys.argv[2]
    port = int(sys.argv[3])
    
    # record packets
    dump(dump_file, host, port)

