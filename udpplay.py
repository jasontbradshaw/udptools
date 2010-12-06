#!/usr/bin/env python

import socket
import base64
import time

def play(dump_file, host, port):
    """
    Plays a given dump file to the specified host and port.  Doesn't play back
    packets at the precise rate received, relying on the ability of any
    receiving client to correctly buffer them and play them back at their
    original rate in some other manner.  Doing this allows a far more efficient
    use of CPU time than playing them back more precisely.
    """

    # TODO: fix the unknown corner case where there is a glitch very near the
    # beginning of playback.  possibly related to the negative-time situation
    # when sleeping.

    # create the socket we'll send packets over
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # we make a function here that makes sending data quick and easy
    addy = (host, port)
    send_packet = lambda data: s.sendto(data, addy)

    # used to store up packets before playing all at once
    buflen = 100
    buf = []

    # read packets from the file and play them to the given address
    with open(dump_file, 'r') as f:
        next_play_time = None
        last_play_time = None
        first_packet_timestamp = None
        for line in f:
            # part before tab is time, part after is data followed by a newline
            line_parts = line.split("\t")
            packet_timestamp = float(line_parts[0])
            packet_data = base64.b64decode(line_parts[1].rstrip()) # strip newline

            # fill buffer, saving times of first and last packets
            if len(buf) < buflen - 1:
                # save the time of the first packet for later
                if len(buf) == 0:
                    first_packet_timestamp = packet_timestamp

                buf.append(packet_data)
                continue
            else:
                # append the last packet, the one read before buflen check failed
                buf.append(packet_data)

                # get first and last packet times and calculate total buffer time
                buffer_time = packet_timestamp - first_packet_timestamp

            # wait until the next buffer should be played. next_play_time is
            # None before the first play.
            if next_play_time is not None:
                # see how much time we should wait before playing the new buffer
                sleep_time = next_play_time - time.time()

                # if we took too much time parsing the last round's packets,
                # play them immediately.
                if sleep_time > 0:
                    time.sleep(sleep_time)

            # send all packets in the buffer, marking time we started playing
            # the buffer.
            last_play_time = time.time()
            map(send_packet, buf)

            # set the next time we play as the number of seconds long the buffer
            # was after the last time we played that buffer.
            next_play_time = last_play_time + buffer_time

            # reset buffer so we'll fill it again
            buf = []

        # wait until we should play what remains in the buffer
        time.sleep(next_play_time - time.time())

        # send what's left in the buffer
        map(send_packet, buf)

def precise_play(dump_file, host, port):
    """
    Plays a given dump file to the specified host and port, sending packets at
    the precise rate they were received.  NOTE: Uses 100% CPU time!
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
    try:
        play(dump_file, host, port)
    except KeyboardInterrupt:
        pass

