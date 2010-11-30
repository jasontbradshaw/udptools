#!/usr/bin/env python

import socket
import base64
import time

def play(dump_file, host, port):
    """
    Plays a given dump file to the specified host and port.
    """

    # create the socket we'll send packets over
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # we make a tuple here to prevent doing so on every loop iteration
    addy = (host, port)

    with open(dump_file, 'r') as f:
        # the alternative to these values are placeholder values.  we would have
        # to check for these every iteration of the loop, slowing things down.
        # this allows us to assume that we can do a valid comparison, and
        # guarantees that the first packet gets sent out immediately.  the
        # variables assume normal values from then on.
        last_line_time = float("inf")

        # this allows us to ignore whether the file is timed with absolute time
        # (starting at some arbitrary date) or relative time (starting at 0.0).
        last_loop_time = float("-inf")

        # every line is a single packet, so we loop over all of them
        for line in f:
            # part before tab is time, part after is data followed by a newline
            line_parts = line.split("\t")
            line_time = float(line_parts[0])
            line_data = base64.b64decode(line_parts[1].rstrip()) # strip newline

            # TODO: find a way to reduce cpu usage while playing packets!
            # Ideas:
            #  - assign time.sleep, base64.b64decode, etc. to variables
            #  - remove rstrip and replace with a slicing off the final char

            # wait until we should play the next packet
            loop_time = time.time()
            while loop_time - last_loop_time < line_time - last_line_time:
                loop_time = time.time()

            # play the next packet
            s.sendto(line_data, addy)

            # update the time variables
            last_line_time = line_time
            last_loop_time = loop_time

if __name__ == "__main__":
    import sys

    # require the dump file
    dump_file = sys.argv[1]

    # also require the host and port to dump it to
    host = sys.argv[2]
    port = int(sys.argv[3])

    # play the file
    play(dump_file, host, port)

