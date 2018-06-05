import SocketServer
import os
import socket
import struct
import threading
import time
import sys

base_dir = ''
appending = {}

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
                #print 'DBG: bytes %d %d %d %d' % (int(length_bytes[0]), int(length_bytes[1]), int(length_bytes[2]), int(length_bytes[3]))
                (c1, c2, c3, c4,) = struct.unpack('>BBBB', length_bytes)
                print 'DBG: bytes %d %d %d %d' % (c1, c2, c3, c4)
                (length,) = struct.unpack('>I', length_bytes)
                print "DBG: waiting for {} bytes from {}".format(length, self.client_address)
                bytes = self.request.recv(length, socket.MSG_WAITALL)
                if len(bytes) < length:
                    break
                (cmd,) = struct.unpack('>c', bytes[0])
                if cmd == 'l':
                    self.do_ls()
                elif cmd == 'g':
                    self.do_get(bytes[1:])
                elif cmd == 'P':
                    self.do_streaming_put(bytes[1:])
                else:
                    self.do_unknown(cmd)
        except Exception as e:
            print 'DBG: exception for {}: {}'.format(self.client_address, e)
            None

    def finish(self):
        print "YO: DOS_Server finish"

    def do_get(self, filename):
        reply = 'TODO: get "{}"\n'.format(filename)
        try:
            f = open(base_dir + '/' + filename, 'r')
            st = os.fstat(f.fileno())
            self.request.sendall(self.frame_bytes(st.st_size))
            reply = 'TODO: fstat results = {}\n'.format(st)
            while True:
                bytes = f.read(32768)
                if bytes == '':
                    break
                self.request.sendall(bytes)
        except Exception as e:
            raise e
        finally:
            try:
                f.close()
            except:
                None

    def do_streaming_put(self, filename):
        try:
            f = open(base_dir + '/' + filename, 'wx')
            reply = 'ok\n'.format(filename)
            self.request.sendall(self.frame_bytes(len(reply)))
            self.request.sendall(reply)
            self.request.setdefaulttimeout(1)
            while True:
                bytes = self.request.recv(32768)
                print 'DBG: do_streaming_put: got %d bytes' % len(bytes)
                if bytes == '':
                    break
                # Note: when writing a real server:
                # "Write a string to the file. There is no return value.""
                f.write(bytes)
        except Exception as e:
            reply = 'ERROR: {}\n'.format(e)
            self.request.sendall(self.frame_bytes(len(reply)))
            self.request.sendall(reply)
            raise e
        finally:
            try:
                f.close()
            except:
                None

    def do_ls(self):
        files = []
        reply = ''
        for file in os.listdir(base_dir):
            files.append(file)
        files.sort()
        for file in files:
            if appending.has_key(file):
                status = 'yes'
            else:
                status = 'no'
            reply = reply + '{}\t{}\n'.format(file, status)
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
