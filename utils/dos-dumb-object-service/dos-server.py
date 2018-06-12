import SocketServer
import os
import socket
import struct
import threading
import time
import sys

## Class from
## https://stackoverflow.com/questions/1312331/using-a-global-dictionary-with-threads-in-python

class ThreadSafeDict(dict) :
    def __init__(self, * p_arg, ** n_arg) :
        dict.__init__(self, * p_arg, ** n_arg)
        self._lock = threading.Lock()

    def __enter__(self) :
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback) :
        self._lock.release()

base_dir = ''
appending = ThreadSafeDict()
debug = True

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True
    pass

class DOS_Server(SocketServer.BaseRequestHandler):
    """
    """

    def setup(self):
        if debug: print 'YO: DOS_Server setup'

    def handle(self):
        if debug: print 'YO: DOS_Server handle top'
        # self.request is the TCP socket connected to the client
        try:
            while True:
                length_bytes = self.request.recv(4, socket.MSG_WAITALL)
                if len(length_bytes) < 4:
                    break
                #if debug: print 'DBG: bytes %d %d %d %d' % (int(length_bytes[0]), int(length_bytes[1]), int(length_bytes[2]), int(length_bytes[3]))
                (c1, c2, c3, c4,) = struct.unpack('>BBBB', length_bytes)
                if debug: print 'DBG: bytes %d %d %d %d' % (c1, c2, c3, c4)
                (length,) = struct.unpack('>I', length_bytes)
                if debug: print 'DBG: waiting for {} bytes from {}'.format(length, self.client_address)
                bytes = self.request.recv(length, socket.MSG_WAITALL)
                if len(bytes) < length:
                    break
                (cmd,) = struct.unpack('>c', bytes[0])
                if cmd == 'a':
                    self.do_streaming_append(bytes[1:])
                elif cmd == 'd':
                    self.do_delete(bytes[1:])
                elif cmd == 'g':
                    self.do_get(bytes[1:])
                elif cmd == 'l':
                    self.do_ls()
                else:
                    self.do_unknown(cmd)
        except Exception as e:
            if debug: print 'DBG: exception for {}: {}'.format(self.client_address, e)
            if debug: print 'DBG: exception for {}: {}'.format(self.client_address, type(e))
            None

    def finish(self):
        if debug: print 'YO: DOS_Server finish'

    def do_streaming_append(self, request):
        am_locked = False
        filename = ''
        try:
            (filename, offset) = request.split("\t")
            offset = int(offset)
            with appending as appending_l:
                # Is file being appended already?
                if appending_l.has_key(filename):
                    raise Exception("%s file is locked" % filename)

                # Open the file for append, find the EOF offset & check it
                f = open(base_dir + '/' + filename, 'a')
                eof_offset = f.tell()
                if offset != eof_offset:
                    raise Exception( \
                        "%s file requested offset %d but eof_offset %d" % \
                        (filename, offset, eof_offset))

                # Not appending elsewhere, offset is ok; let's go
                appending_l[filename] = True
                am_locked = True

            reply = 'ok\n'.format(filename)
            self.request.sendall(self.frame_bytes(len(reply)))
            self.request.sendall(reply)
            last_now = time.time()
            self.reported_offset_w = eof_offset
            while True:
                bytes = ''
                try:
                    bytes = self.request.recv(64*1024)
                except socket.timeout:
                    if eof_offset != self.reported_offset_w:
                        if debug: print 'DBG: do_streaming_append: QQQ timeout: %d' % eof_offset
                        self._send_append_status(eof_offset, eof_offset) # TODO
                    continue
                if debug: print 'DBG: do_streaming_append: got %d bytes' % len(bytes)
                if bytes == '':
                    f.flush()
                    os.fsync(f.fileno())
                    if debug: print 'DBG: do_streaming_append: QQQ socket closed: %d' % eof_offset
                    self._send_append_status(eof_offset, eof_offset) # TODO
                    break
                f.write(bytes)
                eof_offset += len(bytes)
                now = time.time()
                if (now - last_now) > 1:
                    if debug: print 'DBG: do_streaming_append: QQQ periodic: %d' % eof_offset
                    self._send_append_status(eof_offset, eof_offset) # TODO
                    last_now = now
        except Exception as e:
            reply = 'ERROR: {}\n'.format(e)
            if debug: print 'DBG: do_streaming_append: ERROR: %s' % e
            self.request.sendall(self.frame_bytes(len(reply)))
            self.request.sendall(reply)
            raise e
        finally:
            if debug: print 'DBG: do_streaming_append: finally: %s' % filename
            if am_locked:
                with appending as appending_l:
                    del appending_l[filename]
            try:
                ## TODO: try harder, like you're supposed to
                f.flush()
                os.fsync(f.fileno())
            except:
                None
            try:
                f.close()
            except:
                None

    def _send_append_status(self, offset_w, offset_s):
        # written offset \t synced offset
        reply = "{}\t{}".format(offset_w, offset_s)
        if debug: print 'DBG: do_streaming_append: %s' % reply
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        self.reported_offset_w = offset_w

    def do_get(self, request):
        """
        Input: file-name\tstarting-offset\tbytes-desired

        If the bytes-desired field is 0, then send the entire file.

        Output: Entire/partial file contents in a single frame.
        """
        try:
            (filename, offset, wanted) = request.split("\t")
            offset = int(offset)
            offset = max(0, offset) # negative offset -> 0
            wanted = int(wanted)
            if debug: print 'DBG: file %s offset %d wanted %d' % (filename, offset, wanted)
            f = open(base_dir + '/' + filename, 'r')
            st = os.fstat(f.fileno())
            file_size = st.st_size
            if wanted == 0:
                wanted = file_size
            remaining = min(wanted, file_size - offset)
            remaining = max(0, remaining)
            self.request.sendall(self.frame_bytes(remaining))
            if debug: print 'DBG: get file %s frame size %d' % (filename, remaining)
            f.seek(offset)
            if debug: print 'DBG: get file %s offset %d' % (filename, offset)
            while True:
                if remaining == 0:
                    break
                to_read = min(remaining, 32768)
                bytes = f.read(to_read)
                if to_read != len(bytes):
                    if debug: print 'HEY, should never happen: wanted %d bytes but got %d' %\
                        (to_read, len(bytes))
                    sys.exit(66)
                self.request.sendall(bytes)
                if debug: print 'DBG: sent %d bytes' % len(bytes)
                remaining -= len(bytes)
        except Exception as e:
            raise e
        finally:
            try:
                f.close()
            except:
                None

    def do_delete(self, filename):
        path = base_dir + '/' + filename
        try:
            os.unlink(path)
            reply = 'ok'
        except:
            reply = 'ERROR'
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        if debug: print 'REPLY: {}'.format(reply)

    def do_ls(self):
        """
        Output: 0 or more lines of ASCII text:

        file-name\tfile-size\tstatus-currently-appending-yes-or-no\n
        """
        files = []
        reply = ''
        for file in os.listdir(base_dir):
            files.append(file)
        files.sort()
        for file in files:
            with appending as appending_l:
                if appending_l.has_key(file):
                    status = 'yes'
                else:
                    status = 'no'
            stat = os.stat(base_dir + "/" + file)
            reply = reply + '{}\t{}\t{}\n'.format(file, stat.st_size, status)
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        if debug: print 'REPLY: {}'.format(reply)

    def do_unknown(self, cmd):
        reply = 'ERROR: unknown command "{}"\n'.format(cmd)
        self.request.sendall(self.frame_bytes(len(reply)))
        self.request.sendall(reply)
        if debug: print 'REPLY: {}'.format(reply)

    def frame_bytes(self, bytes):
        return struct.pack('>I', bytes)

if __name__ == "__main__":
    (_, base_dir) = sys.argv

    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "localhost", 9999

    socket.setdefaulttimeout(1)
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
