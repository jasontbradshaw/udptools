import socket
import base64
import time
import multiprocessing as mp

class AlreadyRunningError(Exception):
    """
    Raised when a dump is attempted while the object is already dumping, or when
    a play is attempted when the object is already playing.
    """

class PacketParseError(Exception):
    """
    Raised when a packet couldn't be successfully parsed.
    """

class UDPPlay:
    def __init__(self):
        self.__proc = None

    def is_running(self):
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
        if self.is_running():
            raise AlreadyRunningError("Unable to start a new play process while"
                    " one is already running.  Stop playback first!")

        args = (dump_file, host, port, begin_time, end_time)
        self.__proc = mp.Process(target=self.__play_loop, args=args)

        self.__proc.start()

    def stop(self):
        """
        Stop any current playback.  Does nothing if nothing is playing.
        """

        # only terminate the process if it's playing
        if self.is_running():
            self.__proc.terminate()

        # only join the process if it's not 'None'
        if self.__proc is not None:
            self.__proc.join()

        # make sure the process was killed
        assert not self.is_running()

    def __parse_packet(self, packet):
        """
        Splits packet into a time and some data. The part before the tab
        character is the time, the part after is data followed by a newline.
        Returns a tuple of (floating point timestamp, binary data string).
        Raises a PacketParseError if the packet could not be sucessfully parsed.
        """

        try:
            parts = packet.split("\t")
            assert len(parts) == 2
        except AssertionError:
            raise PacketParseError("Could not split timestamp and data in "
                    "packet '%s'" % packet)

        try:
            timestamp = float(parts[0])
            assert timestamp >= 0.0
        except AssertionError:
            raise PacketParseError("Got invalid timestamp '%.10f' from packet "
                    "'%s'" % (timestamp, packet))
        except ValueError:
            raise PacketParseError("Invalid float format for timestamp in "
                    "packet '%s'" % packet)

        try:
            # the rstrip call removes the trailing newline
            data = base64.b64decode(parts[1].rstrip())
        except Exception, e:
            raise PacketParseError("Invalid data format in packet '%s'\n"
                    "Got error: '%s'" % (packet, str(e)))

        return timestamp, data

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

        # read packets from the file and play them to the given address
        with open(dump_file, 'r') as f:
            # seek to the start position if a relevant begin_time was set
            if begin_time > 0:
                start_byte = self.find_timestamp_position(dump_file, begin_time)
                f.seek(start_byte)

            next_play_time = None
            last_play_time = None
            first_packet_timestamp = None
            for line in f:
                # get the packet pieces so we can send them over the socket. if
                # we fail to parse the packet, skip it.
                try:
                    packet_timestamp, packet_data = self.__parse_packet(line)
                except PacketParseError, ppe:
                    # TODO: log this instead
                    print ppe
                    continue

                # stop playback before the specified end time
                if end_time is not None and packet_timestamp > end_time:
                    break

                # add the packet to the buffer, since every packet must be added
                # eventually
                buf.append(packet_data)

                # save times of first and last packets
                if len(buf) < buflen:
                    # save the time of the first packet for later
                    if len(buf) == 1:
                        first_packet_timestamp = packet_timestamp

                    # keep filling until we're full
                    continue
                else:
                    # get first and last packet times and calculate total buffer
                    # time.
                    buffer_time = packet_timestamp - first_packet_timestamp

                # wait until the next buffer should be played. next_play_time is
                # None before the first play.
                if next_play_time is not None:
                    # see how much time we should wait before playing the buffer
                    sleep_time = next_play_time - time.time()

                    # only sleep if we didn't take longer than the buflen to
                    # parse this round's packets.
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

            # TODO: make this return 'None' when the line isn't found
            # TODO: refactor this method to be more intuitive!

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

            # add the part we skipped to the middle position so it lines up with
            # the nearest packet boundary.
            middle_pos = middle_pos + len(raw_skipped)

            # if we skipped more than half of the remaining bytes being checked
            # against, we're not going to find a different time than what we
            # already found, so we return the special value -1 to signal that
            # the previous middle byte position should be returned.
            if len(raw_skipped) >= (end_pos - start_pos) / 2:
                return -1

            # get the next line and parse out the timestamp
            raw_line = fd.readline()
            raw_parts = raw_line.split("\t")
            line_timestamp = float(raw_parts[0])

            # search left half of range
            if timestamp < line_timestamp:
                result = find_recursive(fd, timestamp, start_pos, middle_pos)
                if result < 0:
                    return middle_pos
                return result

            # search right half of range
            else:
                result = find_recursive(fd, timestamp, middle_pos, end_pos)
                if result < 0:
                    return middle_pos
                return result

        # run our recursive function and return the seek position
        with open(dump_file, 'r') as f:
            return find_recursive(f, timestamp, 0, None)

class UDPDump:
    def __init__(self):
        self.__proc = None

    def is_running(self):
        """
        Return whether there is a currently running dump.
        """

        return self.__proc is not None and self.__proc.is_alive()

    def dump(self, dump_file, host, port, max_packet_size=16384):
        """
        Dumps any UDP traffic from the given host and port to the given file.
        max_packet_size is the size in bytes of the largest packet able to be
        received.
        """

        # raise an exception if there's already a dump running
        if self.is_running():
            raise AlreadyRunningError("Unable to start a new dump while one is "
                    "already running.  Stop the current dump first!")

        args = (dump_file, host, port, max_packet_size)
        self.__proc = mp.Process(target=self.__dump_loop, args=args)

        self.__proc.start()

    def stop(self):
        """
        Terminate the currently running dump, if one is running.
        """

        if self.is_running():
            self.__proc.terminate()

        if self.__proc is not None:
            self.__proc.join()

        # make sure the process was killed
        assert not self.__proc.isalive()

    def __dump_loop(self, dump_file, host, port, max_packet_size):
        """
        Dump UDP traffic to a file.
        """

        # set up the socket we're capturing packets from
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((host, port))

        # the format of the lines written to the file, 10 place timestamp, tab,
        # data.
        file_format = "%.10f\t%s\n"

        # dump packets to the file
        with open(dump_file, 'w') as f:

            # time codes are relative to the first packet received, which has
            # time 0.0. we set it after receive so any delay before traffic
            # doesn't show up in the first timestamp.
            first_packet_time = None
            while 1:
                # receive a packet and save the time we received it
                raw_packet = s.recv(max_packet_size)
                packet_recv_time = time.time()

                # encode the raw binary packet data into a base64 string
                packet_data = base64.b64encode(raw_packet)

                # determine the time we'll write to file for this packet. this
                # ensures that the first packet is always marked as 0.0.
                packet_time = None
                if first_packet_time is None:
                    # mark first received packet time
                    first_packet_time = packet_recv_time
                    packet_time = 0.0
                else:
                    # calculate time since first packet
                    packet_time = packet_recv_time - first_packet_time

                # make sure we've got a valid packet time
                assert packet_time is not None
                assert packet_time >= 0.0

                # write time elapsed from start plus data
                f.write(file_format % (packet_time, packet_data))
