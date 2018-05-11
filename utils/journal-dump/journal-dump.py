#!/usr/bin/env python2

from struct import unpack
import sys

try:
    f = open(sys.argv[1], 'r')
except e:
    print 'cannot open %s: %s' % (sys.argv[0], e)
    sys.exit(7)
    
while True:
    b = f.read(4)
    if len(b) != 4:
        break

    (num_bytes,) = unpack('>L', b)
    # print "DBG: num_bytes = %d" % num_bytes
    b = f.read(num_bytes)
    if len(b) != num_bytes:
        print 'Error: wanted %d bytes but got %d instead' % (num_bytes, len(b))
        sys.exit(1)
    offset = 12
    (version, op, num_int, num_string, tag,) = unpack('>BBBBQ', b[0:offset])

    ints = []
    strings_len = []
    strings = []

    for i in range(num_int):
        (n,) = unpack('>Q', b[offset+(8*i) : offset+(8*i) + 8])
        ints.append(n)
    offset += (num_int * 8)

    for i in range(num_string):
        (n,) = unpack('>Q', b[offset+(8*i) : offset+(8*i) + 8])
        strings_len.append(n)
    offset += (num_string * 8)
    b = b[offset:]
    for i in range(num_string):
        strings.append(b[0:strings_len[i]])
        b = b[strings_len[i]:]

    print 'version=%d op=%d tag=%d ints=%s strings=%s\n' % (version, op, tag, ints, strings)

print 'EOF'
sys.exit(0)
