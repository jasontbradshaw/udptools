#!/usr/bin/env python

import socket
import base64
import time
import multiprocessing as mp

class AlreadyDumpingError(Exception):
    """
    Raised when a dump is attempted while one is already running.
    """

class UDPDump:
    def __init__(self):
        self.__proc = None

    def is_dumping(self):
        """
        Return whether there is a currently running dump.
        """

        return self.__proc is not None and self.__proc.is_alive()

    def dump(self, dump_file, host, port, max_packet_size=16384):
        """
        Dumps any UDP traffic from the given host and port to the given file.
        max_packet_size is the size in bytes of the larges packet able to be
        received.
        """

        # raise an exception if there's already a dump running
        if self.is_dumping():
            raise AlreadyDumpingError("Unable to start a new dump while one is "
                    "already running.  Stop the current dump first!")

        args = (dump_file, host, port, max_packet_size)
        self.__proc = mp.Process(target=self.__dump_loop, args=args)

        self.__proc.start()

    def stop(self):
        """
        Terminate the currently running dump, if one is running.
        """

        if self.is_dumping():
            self.__proc.terminate()

        if self.__proc is not None:
            self.__proc.join()

    def __dump_loop(self, dump_file, host, port, max_packet_size):
        """
        Dump UDP traffic to a file.
        """

        # set up the socket we're capturing packets from
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((host, port))

        # the format of the lines written to the file, timestamp + data
        file_format = "%.10f\t%s\n"
        
        # dump packets to the file
        with open(dump_file, 'w') as f:
            
            # time codes are relative to the first packet received, which has time
            # 0.0.  we set it after recv so any delay before traffic doesn't show up
            # in the start time.
            first_packet_time = None
            while 1:
                # receive a packet and save the time we received it
                raw_packet = s.recv(max_packet_size)
                recv_time = time.time()
                
                # encode the raw binary packet data into a base64 string
                data = base64.b64encode(raw_packet)
                
                # mark first received packet time and set its time in file to '0.0'
                if first_packet_time is None:
                    first_packet_time = recv_time
                    f.write(file_format % (0.0, data))
                    continue
                
                # write time elapsed from start plus data
                f.write(file_format % (recv_time - first_packet_time, data))

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
        while 1:
            time.sleep(0.1)
    except KeyboardInterrupt:
        udpdump.stop()

