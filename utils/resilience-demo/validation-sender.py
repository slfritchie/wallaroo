
from end_points import (
						 Reader,
						 sequence_generator
	)
from integration import (
						 Sender
	)

import sys
import time

wallaroo_hostsvc = sys.argv[1]
num_start = int(sys.argv[2])
num_end   = int(sys.argv[3])
batch_size = int(sys.argv[4])
interval = float(sys.argv[5])

print 'wallaroo_hostsvc = %s, num_start = %d, num_end = %d' % (wallaroo_hostsvc, num_start, num_end)

sender = Sender(wallaroo_hostsvc,
				Reader(sequence_generator(start=num_start, stop=num_end)),
                batch_size=batch_size, interval=interval, reconnect=True)
sender.run()
sys.exit(0)