#!/usr/bin/env python

import socket
import base64
import time
import multiprocessing as mp

class AlreadyPlayingError(Exception):
    """
    Raised when 'play' is called while the process is already playing.
    """

class UDPPlay:
    def __init__(self):
        self.__proc = None

    def is_playing(self):
        """
        Returns whether a file is currently playing.
        """

        return self.__proc is not None and self.__proc.is_alive()

    def play(self, dump_file, host, port, begin_time=0, end_time=None):
        """
        Plays the given file to the given host and port.
        """

        # make sure that we don't start a new process while there's one already
        # running.
        if self.is_playing():
            raise AlreadyPlayingError("Unable to start a new play process while"
                    " one is already running.  Stop playback first!")

        args = (dump_file, host, port, begin_time, end_time)
        self.__proc = mp.Process(target=self.__play_loop, args=args)

        self.__proc.start()

    def stop(self):
        """
        Stop any current playback.  Does nothing if nothing is playing.
        """

        # only terminate the process if it's playing
        if self.is_playing():
            self.__proc.terminate()

        # only join the process if it's not 'None'
        if self.__proc is not None:
            self.__proc.join()

    def __play_loop(self, dump_file, host, port, begin_time, end_time):
        """
        Plays a given dump file to the specified host and port.  Doesn't play
        back packets at the precise rate received, relying on the ability of any
        receiving client to correctly buffer them and play them back at their
        original rate in some other manner.  Doing this allows a far more
        efficient use of CPU time than playing them back more precisely.
        """

        # create the socket we'll send packets over
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # we make a function here that makes sending data quick and easy
        addy = (host, port)
        send_packet = lambda data: s.sendto(data, addy)

        # used to store up packets before playing all at once
        buflen = 100
        buf = []

        # if necessary, find the start position before beginning playback
        start_position = None
        if begin_time > 0:
            print "start seek..."
            start_position = self.find_timestamp_position(dump_file, begin_time)
            print "end seek."

        # read packets from the file and play them to the given address
        with open(dump_file, 'r') as f:

            # seek to the start position if one was set
            if start_position is not None:
                f.seek(start_position)

            next_play_time = None
            last_play_time = None
            first_packet_timestamp = None
            for line in f:
                # part before tab is time, part after is data followed by a '\n'
                line_parts = line.split("\t")
                packet_timestamp = float(line_parts[0])

                # the rstrip call removes the trailing newline
                packet_data = base64.b64decode(line_parts[1].rstrip())

                # stop playback before the specified end time
                if end_time is not None and packet_timestamp > end_time:
                    break

                # add the packet to the buffer, since every packet must be added
                # eventually
                buf.append(packet_data)

                # save times of first and last packets
                if len(buf) < buflen - 1:
                    # save the time of the first packet for later
                    if len(buf) == 1:
                        first_packet_timestamp = packet_timestamp

                    # keep filling until we're one packet from full 
                    continue
                else:
                    # get first and last packet times and calculate total buffer
                    # time.
                    buffer_time = packet_timestamp - first_packet_timestamp

                # wait until the next buffer should be played. next_play_time is
                # None before the first play.
                if next_play_time is not None:
                    # see how much time we should wait before playing the new
                    # buffer.
                    sleep_time = next_play_time - time.time()

                    # if we took too much time parsing the last round's packets,
                    # play them immediately.
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                # send all packets in the buffer, marking time we started
                # playing the buffer.
                last_play_time = time.time()
                map(send_packet, buf)

                # set the next time we play as the number of seconds long the
                # buffer was after the last time we played that buffer.
                next_play_time = last_play_time + buffer_time

                # reset buffer so we'll fill it again
                buf = []

            # wait until we should play what remains in the buffer.
            # next_play_time can be 'None' if the buffer wasn't filled at least
            # once while iterating over the file.
            if next_play_time is not None:
                time.sleep(next_play_time - time.time())

            # send what's left in the buffer
            map(send_packet, buf)

    def __precise_play_loop(self, dump_file, host, port):
        """
        Plays a given dump file to the specified host and port, sending packets
        at the precise rate they were received.  NOTE: Uses 100% CPU time!
        """

        # create the socket we'll send packets over
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # we make a tuple here to prevent doing so on every loop iteration
        addy = (host, port)

        with open(dump_file, 'r') as f:
            # the alternative to these values are placeholder values.  we would
            # have to check for these every iteration of the loop, slowing
            # things down.  this allows us to assume that we can do a valid
            # comparison, and guarantees that the first packet gets sent out
            # immediately.  the variables assume normal values from then on.
            last_line_time = float("inf")

            # this allows us to ignore whether the file is timed with absolute
            # time (starting at some arbitrary date) or relative time (starting
            # at 0.0).
            last_loop_time = float("-inf")

            # every line is a single packet, so we loop over all of them
            for line in f:
                # part before tab is time, part after is data, then a newline
                line_parts = line.split("\t")
                line_time = float(line_parts[0])

                # strip the trailing newline from the data before encoding
                line_data = base64.b64decode(line_parts[1].rstrip())

                # wait until we should play the next packet
                loop_time = time.time()
                while loop_time - last_loop_time < line_time - last_line_time:
                    loop_time = time.time()

                # play the next packet
                s.sendto(line_data, addy)

                # update the time variables
                last_line_time = line_time
                last_loop_time = loop_time

    def find_timestamp_position(self, dump_file, timestamp):
        """
        Finds the first position directly before the given position in the given
        dump file.  Returns the file position in bytes such that the next read
        from that position would be aligned with the beginning of the
        appropriate line, ie. the first line with a timestamp greater than or
        equal to the specified timestamp.  Returns 'None' if the requested
        timestamp could not be found in the file (was too far in the future, for
        example).
        """

        def find_recursive(fd, timestamp, start_pos, end_pos):
            """
            A binary search algorithm for a timestamped line in the given file.
            Returns the file position (in bytes) of the beginning of the
            requested line, or 'None' if that line doesn't exist.  start_pos is
            the starting byte position to consider, end_pos the end position.
            An end_pos of 'None' means "The end of the file."
            """

            # if end_pos is None, set it instead to the end position of the file
            # by 'seek'ing to the end, then 'tell'ing for the position.
            if end_pos is None:
                OS_SEEK_END = 2
                fd.seek(0, OS_SEEK_END)
                end_pos = fd.tell()

            # go to the middle of the two given positions
            middle_pos = (end_pos + start_pos) / 2
            fd.seek(middle_pos)

            # read a line to skip to the start of the next line. we save the
            # skipped part so we can use its length to calculate the beginning
            # of the nearest line.
            raw_skipped = fd.readline()

            # if we skipped more than half of the remaining bytes being checked
            # against, we're not going to find a differtn time than what we
            # already found, so we return the special value -1 to signal that
            # the previous byte position should be returned.
            if len(raw_skipped) >= (end_pos - start_pos) / 2:
                return -1

            # get the next line and parse out the timestamp
            raw_line = fd.readline()
            raw_parts = raw_line.split("\t")
            line_timestamp = float(raw_parts[0])

            # search left half of range
            if timestamp < line_timestamp: 
                result = find_recursive(fd, timestamp, start_pos, middle_pos +
                        len(raw_skipped))
                if result < 0:
                    return middle_pos + len(raw_skipped)
                return result

            # search right half of range
            elif timestamp > line_timestamp:
                result = find_recursive(fd, timestamp, middle_pos +
                        len(raw_skipped), end_pos)
                if result < 0:
                    return middle_pos + len(raw_skipped)
                return result

            # we happened to find the exact point requested
            else: 
                return middle_pos + len(raw_skipped)

        # run our recursive function and return the seek position
        with open(dump_file, 'r') as f:
            return find_recursive(f, timestamp, 0, None)

if __name__ == "__main__":
    import sys
    import optparse

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
        while udpplay.is_playing():
            time.sleep(0.1)
    except KeyboardInterrupt:
        udpplay.stop()

