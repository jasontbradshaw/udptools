import os
import select
import socket
import threading

from base64 import b64encode, b64decode
from time import time, sleep

def play(f, sock, begin_time=0, end_time=None, player=None):
    """
    Plays a given file object to the specified socket. Doesn't play back
    packets at the precise rate received, relying on the ability of any
    receiving client to correctly buffer them and play them back at their
    original rate in some other manner. Doing this allows a far more
    efficient use of CPU time than playing them back more precisely.
    """

    # store the state we look for in a local var to reduce lookup penalty
    STOPPED = Player.STOPPED

    # a convenience function for sending a packet over our socket
    send_packet = lambda packet: sock.sendall(packet.data)

    # used to store up packets before playing all at once
    buflen = 100
    buf = []

    # variables used for timing playback
    next_play_time = None
    last_play_time = None

    # seek to the start position if a relevant begin_time was set
    if begin_time > 0:
        start_byte = find_timestamp(f, begin_time)
        f.seek(start_byte)

    for line in f:
        # end playback if a stop has been signaled on a given Player object
        if player is not None and player.state == STOPPED:
            break

        # parse the packet so we can send it over the socket. if we fail
        # to parse the packet, skip it.
        try:
            packet = Packet(line)
        except ValueError, e:
            # TODO: log this instead
            print e
            continue

        # stop playback immediately before the specified end time
        if end_time is not None and packet.timestamp >= end_time:
            break

        # add the packet to the buffer
        buf.append(packet)

        # keep filling the buffer until it's full
        if len(buf) < buflen:
            continue

        # wait until the next buffer should be played. next_play_time is
        # None before the first play.
        if next_play_time is not None:
            # only sleep if there's time between the last play time and
            # the next calculated playback time.
            if next_play_time - time() > 0:
                sleep(sleep_time)

        # calculate total buffer time, done here to account for the
        # length of this calculation.
        buffer_time = buf[-1].timestamp - buf[0].timestamp

        # send all packets in the buffer, marking time we started
        # playing the buffer.
        last_play_time = time()
        map(send_packet, buf)

        # set the next time we play as the number of seconds long the
        # buffer was after the last time we played that buffer.
        next_play_time = last_play_time + buffer_time

        # empty the buffer so it can be filled again
        del buf[:]

    # wait until we should play what remains in the buffer. next_play_time
    # can be 'None' if the buffer wasn't filled at least once while
    # iterating over the file.
    if next_play_time is not None:
        sleep(next_play_time - time())

    # send what's left in the buffer
    map(send_packet, buf)

def record(f, sock, max_packet_size, recorder=None):
    """Record UDP traffic to the given writable file object."""

    # store the recording flag for quicker local reference
    RECORDING = Recorder.RECORDING

    # the format of the file's lines: 10 place timestamp, tab, data
    file_format = "%.10f\t%s\n"

    # time codes are relative to the first packet received, which has
    # time 0.0. we set it after receive so any delay before traffic
    # doesn't show up in the first timestamp.
    first_packet_time = None

    # write packets to the file
    while recorder is not None and recorder.state == RECORDING:
        # attempt to recv from the socket, retrying if nothing was read
        readable, _, _ = select.select([sock], [], [], 0.1)
        if len(readable) == 0:
            continue

        # save the time we're receiving the packet, then recv it
        packet_recv_time = time.time()
        raw_packet = sock.recv(max_packet_size)

        # encode the raw binary packet data into a base64 string
        packet_data = b64encode(raw_packet)

        # determine the time we'll write to file for this packet. this
        # ensures that the first packet is always marked as 0.0.
        if first_packet_time is None:
            # mark first received packet time
            first_packet_time = packet_recv_time
            packet_time = 0.0
        else:
            # calculate time since first packet
            packet_time = packet_recv_time - first_packet_time

        # write time elapsed from start plus data
        f.write(file_format % (packet_time, packet_data))

def find_timestamp(fname, timestamp):
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
                line_timestamp = Packet.parse_packet(line)[0]
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

class Packet:
    """Holds packet information, namely timestamp and raw data."""

    def __init__(self, raw_data):
        self.timestamp, self.data = Packet.parse_packet(raw_data)

    @staticmethod
    def parse_packet(raw_data):
        """
        Splits a raw packet string into a timestamp and some data. The part
        before the tab character is the time, the part after is data followed by
        a newline. Returns a tuple of (floating point timestamp, binary data
        string). Raises a ValueError if the packet could not be sucessfully
        parsed.
        """

        try:
            parts = raw_data.split("\t")
            assert len(parts) == 2
        except AssertionError:
            raise ValueError("Could not split timestamp and data in "
                    "packet '%s'" % raw_data)

        try:
            timestamp = float(parts[0])
            assert timestamp >= 0.0
        except AssertionError:
            raise ValueError("Got invalid timestamp '%.10f' from packet "
                    "'%s'" % (timestamp, raw_data))
        except ValueError:
            raise ValueError("Invalid float format for timestamp in "
                    "packet '%s'" % raw_data)

        try:
            # the rstrip call removes the trailing newline
            data = b64decode(parts[1].rstrip())
        except Exception, e:
            raise ValueError("Invalid data format in packet '%s'\n"
                    "Got error: '%s'" % (raw_data, str(e)))

        return timestamp, data

class Player(object):

    # the different states the object can be in
    PLAYING = "playing"
    STOPPED = "stopped"

    def __init__(self, fname, address):
        self.__fname = os.path.abspath(fname)
        self.__address = address

        # the thread where playback takes place
        self.__proc = None

        # create the internal lock used for changing state, set state to stopped
        self.__lock = threading.Lock()
        self.__state = STOPPED

    @property
    def address(self):
        """The (host, port) tuple packets are sent to."""
        return self.__address

    @property
    def filename(self):
        """The name of the file being played."""
        return self.__fname

    @property
    def state(self):
        """Get the current state of the object: playing, paused, or stopped."""
        with self.__lock:
            return self.__state

    def stop(self, timeout=10):
        """Stop playback and join the underlying thread."""

        # signal that the playback thread should stop
        with self.__lock:
            self.__state = STOPPED

        # join the playback thread and reset it
        self.__proc.join(timeout)

        # make sure we successfully killed the playback thread
        if self.__proc.is_alive():
            raise IOError("Failed to join playback thread")

        # reset the thread reference
        self.__proc = None

    def play(self, begin_time=0, end_time=None):
        """
        Play the file. Playing continues until stop() is called. Returns
        immediately: True if playback was started, False if playback was already
        happening.
        """

        with self.__lock:
            # don't do anything if it's already playing a file
            if self.__state != STOPPED:
                return False

            # create the socket, binding it to the address
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(self.__address)

            # create a function that opens and plays the internal file
            def play_thread():
                with open(self.__fname, 'r') as f:
                    play(f, sock, begin_time, end_time, player=self)

            # build the playback thread
            self.__proc = threading.Thread(play_thread)

            # set that we're playing before we start the thread
            self.__state = PLAYING

            # start the playback thread
            self.__proc.start()

            # signal that playback was started
            return True

class Recorder(object):

    RECORDING = "recording"
    STOPPED = "stopped"

    def __init__(self, fname, address):
        self.__fname = os.path.abspath(fname)
        self.__address = address

        self.__proc = None

        self.__lock = threading.Lock()
        self.__state = STOPPED

    @property
    def address(self):
        """The (host, port) tuple packets are recorded from."""
        return self.__address

    @property
    def filename(self):
        """The name of the file being recorded to."""
        return self.__fname

    @property
    def state(self):
        """Get the current state of the object: recording or stopped."""
        with self.__lock:
            return self.__state

    def stop(self, timeout=10):
        """Stop recording and join the underlying thread."""

        with self.__lock:
            self.__state = STOPPED

        self.__proc.join(timeout)

        if self.__proc.is_alive():
            raise IOError("Failed to join recording thread")

        self.__proc = None

    def record(self, max_packet_size=16384):
        """
        Record any UDP traffic from an address to a file. max_packet_size is the
        size in bytes of the largest packet able to be received.
        """

        with self.__lock:
            # don't do anything if it's already recording a file
            if self.__state != STOPPED:
                return False

            # set up the socket we're capturing packets from
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False) # non-blocking so we can use select on it
            sock.bind(self.__address)

            # create a function that records to the file, appending if specified
            def record_thread():
                with open(self.__fname, 'w') as f:
                    record(f, sock, max_packet_size, recorder=self)

            self.__proc = threading.Thread(record_thread)
            self.__state = RECORDING
            self.__proc.start()

            return True
