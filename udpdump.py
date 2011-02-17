#!/usr/bin/env python

import socket
import base64
import time
import multiprocessing as mp

from udptools_exceptions import AlreadyRunningError

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

if __name__ == "__main__":
    import sys

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
