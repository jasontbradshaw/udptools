import base64
import os
import socket
import threading
import time

class Player:

    def __init__(self, fname, address):
        self.fname = os.path.abspath(fname)
        self.address = address
        self.proc = None

        self.pause = threading.Event()
        self.stop = threading.Event()

    def play(self, begin_time=0, end_time=None):
        """Start the play process. Blocks until the operation finishes."""

        # create the socket we'll send packets over
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(self.address)

        # open the file so we can pass it to the thread
        with open(self.fname, 'r') as f:
            self.__play(f, s, begin_time, end_time)

        # signal that playback completed
        return True

    def __parse_packet(self, packet):
        """
        Splits packet into a time and some data. The part before the tab
        character is the time, the part after is data followed by a newline.
        Returns a tuple of (floating point timestamp, binary data string).
        Raises a ValueError if the packet could not be sucessfully parsed.
        """

        try:
            parts = packet.split("\t")
            assert len(parts) == 2
        except AssertionError:
            raise ValueError("Could not split timestamp and data in "
                    "packet '%s'" % packet)

        try:
            timestamp = float(parts[0])
            assert timestamp >= 0.0
        except AssertionError:
            raise ValueError("Got invalid timestamp '%.10f' from packet "
                    "'%s'" % (timestamp, packet))
        except ValueError:
            raise ValueError("Invalid float format for timestamp in "
                    "packet '%s'" % packet)

        try:
            # the rstrip call removes the trailing newline
            data = base64.b64decode(parts[1].rstrip())
        except Exception, e:
            raise ValueError("Invalid data format in packet '%s'\n"
                    "Got error: '%s'" % (packet, str(e)))

        return timestamp, data

    def __play(self, f, sock, address, begin_time, end_time):
        """
        Plays a given file object to the specified address. Doesn't play back
        packets at the precise rate received, relying on the ability of any
        receiving client to correctly buffer them and play them back at their
        original rate in some other manner. Doing this allows a far more
        efficient use of CPU time than playing them back more precisely.
        """

        # bind the send function to a local variable to reduce lookup penalty
        sendall = sock.sendall

        # used to store up packets before playing all at once
        buflen = 100
        buf = []

        # seek to the start position if a relevant begin_time was set
        if begin_time > 0:
            start_byte = self.__find_timestamp_smart(f, begin_time)
            f.seek(start_byte)

        # read packets from the file and play them to the given address
        next_play_time = None
        last_play_time = None
        first_packet_timestamp = None
        for line in f:
            # get the packet pieces so we can send them over the socket. if
            # we fail to parse the packet, skip it.
            try:
                packet_timestamp, packet_data = self.__parse_packet(line)
            except ValueError, e:
                # TODO: log this instead
                print e
                continue

            # stop playback before the specified end time
            if end_time is not None and packet_timestamp > end_time:
                break

            # add the packet to the buf (every packet must be added eventually)
            buf.append(packet_data)

            # save times of first and last packets
            if len(buf) < buflen:
                # save the time of the first packet for later
                if len(buf) == 1:
                    first_packet_timestamp = packet_timestamp

                # keep filling until we're full
                continue
            else:
                # get first and last packet times and calculate total buffer time
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
            map(sendall, buf)

            # set the next time we play as the number of seconds long the
            # buffer was after the last time we played that buffer.
            next_play_time = last_play_time + buffer_time

            # reset buffer so we'll fill it again
            del buf[:]

        # wait until we should play what remains in the buffer.
        # next_play_time can be 'None' if the buffer wasn't filled at least
        # once while iterating over the file.
        if next_play_time is not None:
            time.sleep(next_play_time - time.time())

        # send what's left in the buffer
        map(sendall, buf)

    def __find_timestamp_smart(self, fname, timestamp):
        """
        Finds the first position directly before the given position in the given
        file object. Returns the file position in bytes such that the next read
        from that position would return the first line with a timestamp greater
        than or equal to the specified timestamp. If the given timestamp occurs
        before the beginning of the file, returns the first position in the
        file. If the given timestamp is after the end of the file, returns the
        final position in the file.
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
        with open(os.path.abspath(f), 'r') as df:
            # seek to the end of the file and get the file size
            df.seek(0, os.SEEK_END)
            file_size = df.tell()

            # seek to the last guaranteed non-EOF, non-newline character
            df.seek(-2, os.SEEK_END)

            # seek backwards and read up to after the first encountered newline
            reverse_seek_to_newline(df)

            # read final time in the file so we can interpolate other times
            final_timestamp = self.__parse_packet(df.readline())[0]

            # estimate a position for the desired time
            pos_guess = int((1.0 * timestamp / final_timestamp) * file_size)

            # limit the guess to the file's boundaries
            pos_guess = max(min(pos_guess, file_size), 0)

            # seek to the guessed position and determine which way we need to
            # read from there to arrive at the desired position.
            df.seek(pos_guess)

            # TODO: finish him!

    def find_timestamp(self, fname, timestamp):
        """
        Reads every line in a file until it either finds the specified time or
        fails to find it at all. Returns the file position in bytes such that
        the next read from that position would return an entire packet
        containing the first time that fell on or after the given timestamp. If
        the given timestamp occurs after the last timestamp in the file, the
        final position in the file is returned. If it occurs before the file
        starts, the first position in the file is returned.
        """

        with open(os.path.abspath(fname), 'r') as df:
            previous_position = 0
            for line in df:
                # get line timestamp and compare to see whether we should return
                # the previous position. if parse fails, skip the packet.
                try:
                    line_timestamp = self.__parse_packet(line)[0]
                except ValueError:
                    # skip this packet and try the next one
                    break

                # return the previous position if we've exceeded the timestamp
                if line_timestamp >= timestamp:
                    break

                # rotate the current position backwards and continue if we
                # haven't found the given timestamp yet.
                previous_position = df.tell()

        return previous_position

class Recorder:

    def record(self, fname, address, max_packet_size=16384):
        """
        Record any UDP traffic from an address to a file. max_packet_size is the
        size in bytes of the largest packet able to be received.
        """

        # set up the socket we're capturing packets from
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        with open(os.path.abspath(fname), 'w') as f:
            self.__record(f, s, address, max_packet_size)

        # signal that the record operation completed
        return True

    def __record(self, f, sock, address, max_packet_size):
        """Record UDP traffic to the given writable file object."""

        # bind the given socket to the desired address
        sock.bind(address)

        # the format of the file's lines: 10 place timestamp, tab, data
        file_format = "%.10f\t%s\n"

        # time codes are relative to the first packet received, which has
        # time 0.0. we set it after receive so any delay before traffic
        # doesn't show up in the first timestamp.
        first_packet_time = None

        # write packets to the file
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
            f.write(file_format % (packet_time, packet_data))
