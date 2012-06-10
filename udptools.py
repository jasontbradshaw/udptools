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
    @classmethod
    def __init__():
        self.proc = None

    @classmethod
    def is_running():
        """
        Returns whether a file is currently playing.
        """

        return self.proc is not None and self.proc.is_alive()

    @classmethod
    def play(dump_file, host, port, begin_time=0, end_time=None):
        """
        Plays the given file to the given host and port.
        """

        # make sure that we don't start a new process while there's one already
        # running.
        if self.is_running():
            raise AlreadyRunningError("Unable to start a new play process while"
                    " one is already running.  Stop playback first!")

        # create the socket we'll send packets over
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # open the file so we can pass it to the thread
        with open(dump_file, 'r') as f:
            args = (f, s, host, port, begin_time, end_time)
            self.proc = mp.Process(target=self.play_loop, args=args)

            self.proc.start()

    @classmethod
    def stop():
        """
        Stop any current playback.  Does nothing if nothing is playing.
        """

        # only terminate the process if it's playing
        if self.is_running():
            self.proc.terminate()

        # only join the process if it's not 'None'
        if self.proc is not None:
            self.proc.join()

        # make sure the process was killed
        assert not self.proc.is_alive()

    @classmethod
    def parse_packet(packet):
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

    @classmethod
    def play_loop(dump_file, sock, host, port, begin_time, end_time):
        """
        Plays a given dump file object to the specified host and port.  Doesn't
        play back packets at the precise rate received, relying on the ability
        of any receiving client to correctly buffer them and play them back at
        their original rate in some other manner.  Doing this allows a far more
        efficient use of CPU time than playing them back more precisely.
        """

        # make sure we can read from the given file object
        assert dump_file.mode.count("r") > 0

        # make a function to make sending data quick and easy
        addy = (host, port)
        send_packet = lambda data: sock.sendto(data, addy)

        # used to store up packets before playing all at once
        buflen = 100
        buf = []

        # seek to the start position if a relevant begin_time was set
        if begin_time > 0:
            start_byte = self.find_timestamp_smart(dump_file, begin_time)
            dump_file.seek(start_byte)

        # read packets from the file and play them to the given address
        next_play_time = None
        last_play_time = None
        first_packet_timestamp = None
        for line in dump_file:
            # get the packet pieces so we can send them over the socket. if
            # we fail to parse the packet, skip it.
            try:
                packet_timestamp, packet_data = self.parse_packet(line)
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

    @classmethod
    def find_timestamp_smart(dump_file, timestamp):
        """
        Finds the first position directly before the given position in the given
        dump file object. Returns the file position in bytes such that the next
        read from that position would return the first line with a timestamp
        greater than or equal to the specified timestamp. If the given timestamp
        occurs before the beginning of the file, returns the first position in
        the dump file. If the given timestamp is after the end of the file,
        returns the final position in the file.
        """

        # TODO: when ANY exception is encountered, fall back to final_timestamp

        def reverse_seek_to_newline(f):
            """
            Seek backwards in an open file until a newline is encountered,
            leaving the file at the first position after that newline.
            """

            while f.read(1) != "\n":
                # seek backwards twice to regain ground lost by reading
                df.seek(-2, os.SEEK_CUR)

        # read the last line of the file to find the time there
        with open(dump_file, 'r') as df:
            # seek to the end of the file and get the file size
            df.seek(0, os.SEEK_END)
            file_size = df.tell()

            # seek to the last guaranteed non-EOF, non-newline character
            df.seek(-2, os.SEEK_END)

            # seek backwards and read up to after the first encountered newline
            reverse_seek_to_newline(df)

            # read final time in the file so we can interpolate other times
            final_timestamp = self.parse_packet(df.readline())[0]

            # estimate a position for the desired time
            pos_guess = int((1.0 * timestamp / final_timestamp) * file_size)

            # limit the guess to the file's boundaries
            pos_guess = max(min(pos_guess, file_size), 0)

            # seek to the guessed position and determine which way we need to
            # read from there to arrive at the desired position.
            df.seek(pos_guess)

            # TODO: finish him!

    @classmethod
    def find_timestamp(dump_file, timestamp):
        """
        Reads every line in a file until it either finds the specified time or
        fails to find it at all. Returns the file position in bytes such that
        the next read from that position would return an entire packet
        containing the first time that fell on or after the given timestamp. If
        the given timestamp occurs after the last timestamp in the file, the
        final position in the file is returned. If it occurs before the file
        starts, the first position in the file is returned.
        """

        with open(dump_file, 'r') as df:
            previous_position = 0
            for line in df:
                # get line timestamp and compare to see whether we should return
                # the previous position. if parse fails, skip the packet.
                try:
                    line_timestamp = self.parse_packet(line)[0]
                except PacketParseError:
                    # skip this packet and try the next one
                    break

                # return the previous position if we've exceeded the timestamp
                if line_timestamp >= timestamp:
                    break

                # rotate the current position backwards and continue if we
                # haven't found the given timestamp yet.
                previous_position = df.tell()

        return previous_position

class UDPDump:
    @classmethod
    def __init__():
        self.proc = None

    @classmethod
    def is_running():
        """
        Return whether there is a currently running dump.
        """

        return self.proc is not None and self.proc.is_alive()

    @classmethod
    def dump(dump_file, host, port, max_packet_size=16384):
        """
        Dumps any UDP traffic from the given host and port to the given file.
        max_packet_size is the size in bytes of the largest packet able to be
        received.
        """

        # raise an exception if there's already a dump running
        if self.is_running():
            raise AlreadyRunningError("Unable to start a new dump while one is "
                    "already running.  Stop the current dump first!")

        # set up the socket we're capturing packets from
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        with open(dump_file, 'w') as f:
            args = (f, s, host, port, max_packet_size)
            self.proc = mp.Process(target=self.dump_loop, args=args)

            self.proc.start()

    @classmethod
    def stop():
        """
        Terminate the currently running dump, if one is running.
        """

        if self.is_running():
            self.proc.terminate()

        if self.proc is not None:
            self.proc.join()

        # make sure the process was killed
        assert not self.proc.is_alive()

    @classmethod
    def dump_loop(dump_file, sock, host, port, max_packet_size):
        """
        Dump UDP traffic to the given opened-for-writing file object.
        """

        # make sure we can write to the given file object
        assert dump_file.mode.count("w") > 0 or dump_file.mode.count("a") > 0

        # bind the given socket to the desired host and port
        sock.bind((host, port))

        # the format of the lines written to the file, 10 place timestamp, tab,
        # data.
        file_format = "%.10f\t%s\n"

        # time codes are relative to the first packet received, which has
        # time 0.0. we set it after receive so any delay before traffic
        # doesn't show up in the first timestamp.
        first_packet_time = None

        # dump packets to the file
        while 1:
            # receive a packet and save the time we received it
            raw_packet = sock.recv(max_packet_size)
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
            dump_file.write(file_format % (packet_time, packet_data))
