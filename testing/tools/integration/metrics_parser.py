# Copyright 2017 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.

from io import BytesIO, BufferedReader
from struct import unpack


# Message Types
#   1: Connect
#   2: Join
#   3: Metrics

# Format types
#   U8: B
#   U16: H
#   U32: I
#   U64: Q
#   Text: <length>s
#   Array: <size><item1><item2>...

MSG_TYPES = {1: 'connect', 2: 'join', 3: 'metrics'}


def _s(n):
    return '{}s'.format(n)


class MetricsParseError(Exception):
    pass


class MetricsParser(object):
    def parse_record(cls, record):
        if record['type'] == 'connect':
            return cls.parse_connect()
        elif record['type'] == 'join':
            return cls.parse_join(record['payload'])
        elif record['type'] == 'metrics':
            return cls.parse_metrics(record['payload'])
        else:
            raise MetricsParseError(
                "No known parsing logic for record {!r}".format(record))

    def parse_connect(cls):
        return {'type': 'connect'}

    def parse_join(cls, payload=''):
        buf = BufferedReader(BytesIO(payload))
        topic_size = unpack('>I', buf.read(4))[0]
        topic = unpack(_s(topic_size), buf.read(topic_size))[0].decode()
        worker_name_size = unpack('>I', buf.read(4))[0]
        worker_name = unpack(_s(worker_name_size),
                             buf.read(worker_name_size))[0].decode()
        return {'type': 'join', 'topic': topic, 'worker_name': worker_name}

    def parse_metrics(cls, payload):
        buf = BufferedReader(BytesIO(payload))
        event_size = unpack('>I', buf.read(4))[0]
        event = unpack(_s(event_size), buf.read(event_size))[0].decode()
        topic_size = unpack('>I', buf.read(4))[0]
        topic = unpack(_s(topic_size), buf.read(topic_size))[0].decode()
        payload_size = unpack('>I', buf.read(4))[0]
        payload_header = unpack('>I', buf.read(4))[0]
        metric_name_size = unpack('>I', buf.read(4))[0]
        metric_name = unpack(_s(metric_name_size),
                             buf.read(metric_name_size))[0].decode()
        metric_category_size = unpack('>I', buf.read(4))[0]
        metric_category = unpack(_s(metric_category_size),
                                 buf.read(metric_category_size))[0].decode()
        worker_name_size = unpack('>I', buf.read(4))[0]
        worker_name = unpack(_s(worker_name_size),
                             buf.read(worker_name_size))[0].decode()
        pipeline_name_size = unpack('>I', buf.read(4))[0]
        pipeline_name = unpack(_s(pipeline_name_size),
                               buf.read(pipeline_name_size))[0].decode()
        ID = unpack('>H', buf.read(2))[0]
        latency_histogram = [unpack('>Q', buf.read(8))[0] for x in range(65)]
        max_latency = unpack('>Q', buf.read(8))[0]
        min_latency = unpack('>Q', buf.read(8))[0]
        duration = unpack('>Q', buf.read(8))[0]
        end_ts = unpack('>Q', buf.read(8))[0]

        return {
                'type': 'metrics',
                'event': event,
                'topic': topic,
                'payload_size': payload_size,
                'payload_header': payload_header,
                'metric_name': metric_name,
                'metric_category': metric_category,
                'worker_name': worker_name,
                'pipeline_name': pipeline_name,
                'id': ID,
                'latency_hist': latency_histogram,
                'total': sum(latency_histogram),
                'max_latency': max_latency,
                'min_latency': min_latency,
                'duration': duration,
                'end_ts': end_ts
                }


class MetricsData(MetricsParser):
    def __init__(self):
        self.records = []
        self.data = {}

    def load_string_list(self, l):
        b = BytesIO()
        for s in l:
            b.write(s)
        b.seek(0)
        buf = BufferedReader(b)
        self.load_buffer(buf)

    def load_string(self, s):
        buf = BufferedReader(BytesIO(s))
        self.load_buffer(buf)

    def load_buffer(self, buf):
        while True:
            # read header
            header_bs = buf.read(4)
            if not header_bs:
                break
            header = unpack('>I', header_bs)[0]
            # message type
            msg_type_bs = buf.read(1)
            if not msg_type_bs:
                break
            msg_type = unpack('>B', msg_type_bs)[0]
            # payload
            if header:
                payload = buf.read(header-1)
            else:
                payload = b''
            self.records.append({'type': MSG_TYPES.get(msg_type, None),
                                 'payload': payload,
                                 'raw_type': msg_type})

    def parse(self):
        # populate self.data dict...
        for r in self.records:
            p = self.parse_record(r)
            if p['type'] == 'connect':
                continue
            elif p['type'] == 'join':
                topic = self.data.setdefault(p['topic'], {})
                worker = topic.setdefault(p['worker_name'], [])
                worker.append(('join',))
            elif p['type'] == 'metrics':
                topic = self.data.setdefault(p['topic'], {})
                worker = topic.setdefault(p['worker_name'], [])
                worker.append(('metrics', p))
