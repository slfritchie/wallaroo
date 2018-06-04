import SocketServer
import socket
import struct
import threading
import time
import sys

base_dir = ''

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True
    pass

class DOS_Server(SocketServer.BaseRequestHandler):
    """
    """

#    def __init__(self, connected_socket, host_port_tuple, instance_ThreadedTCPServer):
#        print "YO: DOS_Server connected to %s" % str(host_port_tuple)

    def setup(self):
        print "YO: DOS_Server setup"

    def handle(self):
        print "YO: DOS_Server handle top"
        # self.request is the TCP socket connected to the client
        try:
            while True:
                length_bytes = self.request.recv(4, socket.MSG_WAITALL)
                if len(length_bytes) < 4:
                    break
                (length,) = struct.unpack('>I', length_bytes)
                print "DBG: waiting for {} bytes from {}".format(length, self.client_address)
                bytes = self.request.recv(length, socket.MSG_WAITALL)
                if len(bytes) < length:
                    break
                (cmd,) = struct.unpack('>c', bytes[0])
                if cmd == 'l':
                    self.do_ls()
                else:
                    self.do_unknown(cmd)
        except Exception as e:
            print 'DBG: exception for {}: {}'.format(self.client_address, e)
            None

    def finish(self):
        print "YO: DOS_Server finish"

    def do_ls(self):
        reply = 'TODO: implement do_ls({})\n'.format(base_dir)
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        print 'REPLY: {}'.format(reply)

    def do_unknown(self, cmd):
        reply = 'ERROR: unknown command "{}"\n'.format(cmd)
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        print 'REPLY: {}'.format(reply)

    def frame_bytes(self, bytes):
        return struct.pack('>I', bytes)

if __name__ == "__main__":
    (_, base_dir) = sys.argv

    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "localhost", 9999

    server = ThreadedTCPServer((HOST, PORT), DOS_Server)
    server.allow_reuse_address = True
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    while True:
        time.sleep(60)
