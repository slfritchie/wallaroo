
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

sink_hostsvc = sys.argv[1]
expect = int(sys.argv[2])
timeout = float(sys.argv[3])
output_file = sys.argv[4]
print 'sink_hostsvc = %s, expect = %d, timeout = %f output_file %s' % \
	(sink_hostsvc, expect, timeout, output_file)
(sink_host, sink_port) = sink_hostsvc.split(":")

of = open(output_file, 'w')

def extract_fun(got):
	of.write(got)
	return re.sub('.*\(', '(', got)

last_value_0 = '(key_0.TraceID-1.TraceWindow-1,[{}])' \
	.format(','.join((str(expect-v) for v in range(3,-1,-1))))
last_value_1 = '(key_1.TraceID-1.TraceWindow-1,[{}])' \
	.format(','.join((str(expect-v) for v in range(3,-1,-1))))
await_values = (last_value_0, last_value_1)

sink = Sink(sink_host, int(sink_port), mode='framed')
sink.start()

t = SinkAwaitValue(sink, await_values, timeout, extract_fun)
t.start()
t.join()
if t.error:
    raise t.error

of.close()
sys.exit(0)
