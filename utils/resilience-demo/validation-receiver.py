
from end_points import (
						 Reader,
						 Sink,
						 sequence_generator
	)
from control import (
					 SinkAwaitValue
	)

import re
import struct
import sys
import time

def extract_fun(x):
	x = re.sub('.*\[', '[', x)
	x = re.sub('\].*', ']', x)
	return x

sink_hostsvc = sys.argv[1]
expect = int(sys.argv[2])
timeout = float(sys.argv[3])
print 'sink_hostsvc = %s, expect = %d, timeout = %f' % (sink_hostsvc, expect, timeout)

last_value_0 = '[{}]'.format(','.join((str(expect-v) for v in range(6,-2,-2))))
last_value_1 = '[{}]'.format(','.join((str(expect-1-v) for v in range(6,-2,-2))))
# 2 worker even-odd case:
await_values = (last_value_0, last_value_1)

# 1 worker case:
last_value_0 = '[{}]'.format(','.join((str(expect-v) for v in range(3,-1,-1))))
await_values = (last_value_0)

print 'sink_hostsvc = %s, expect = %d' % (sink_hostsvc, expect)

(sink_host, sink_port) = sink_hostsvc.split(":")
print 'host %s port %s' % (sink_host, sink_port)
sink = Sink(sink_host, int(sink_port), mode='framed')
sink.start()

t = SinkAwaitValue(sink, await_values, timeout, extract_fun)
	
t.start()
t.join()
if t.error:
    raise t.error

sys.exit(0)
