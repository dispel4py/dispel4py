# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time


class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start


class MonitoringWrapper(object):

    def __init__(self, baseObject):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.baseObject = baseObject


class ReadTimingWrapper(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.readtime = None
        self.readrate = []

    def _read(self):
        now = time.time()
        if self.readtime:
            self.readrate.append(now - self.readtime)
        self.readtime = now
        return self.baseObject._read()

    def _terminate(self):
        self.log("Average read rate : %s" % (sum(self.readrate) /
                                             float(len(self.readrate))))
        self.baseObject._terminate()


class ProcessTimingPE(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.times_total = 0
        self.times_count = 0

    def process(self, inputs):
        with Timer() as t:
            result = self.baseObject.process(inputs)
        self.times_total += t.secs
        self.times_count += 1
        return result

    def _postprocess(self):
        self.log('Average processing time: %s' % (self.times_total /
                                                  self.times_count))


class EventTimestamp(object):

    def __init__(self, name):
        self.name = name
        self.data = {}

    def __str__(self):
        return 'EventTimestamp(%s, %s, %.5f, %.5f)' \
            % (self.name, self.data, self.start, self.end)

    def __repr__(self):
        return self.__str__()

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()

from dispel4py.utils import total_size


class TimestampEventsPE(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self._monitoring_events = []

    def write(self, name, data):
        with EventTimestamp('write') as t:
            t.data['output'] = name
            t.data['size'] = total_size(data)
            self._monitoring_events.append(t)
            return self.baseObject.write(name, data)

    def preprocess(self):
        with EventTimestamp('preprocess') as t:
            self._monitoring_events.append(t)
            return self.baseObject.preprocess()

    def process(self, inputs):
        with EventTimestamp('process') as t:
            self._monitoring_events.append(t)
            return self.baseObject.process(inputs)

    def postprocess(self):
        with EventTimestamp('postprocess') as t:
            self._monitoring_events.append(t)
            return self.baseObject.postprocess()


class TimestampEventsWrapper(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.events = []
        self.baseObject.pe = TimestampEventsPE(self.baseObject.pe)

    def _write(self, name, data):
        with EventTimestamp('write') as t:
            t.data['output'] = name
            t.data['size'] = total_size(data)
            self.events.append(t)
            self.baseObject._write(name, data)
        # print('>>> %s WRITE: %.6f s' % (self.baseObject.pe.id,
        #                                  (t.end - t.start)))
        self.write_events()

    def _read(self):
        with EventTimestamp('read') as t:
            self.events.append(t)
            obj = self.baseObject._read()
        try:
            data, status = obj
            t.data['input'] = list(data.keys())
        except:
            # if the data is not a dictionary (could be None)
            pass
        self.write_events()
        # print('>>> %s READ: %.6f s' % (self.baseObject.pe.id,
        #                                 (t.end - t.start)))
        return obj

    def _terminate(self):
        with EventTimestamp('terminate') as t:
            self.events.append(t)
            self.baseObject._terminate()
        self.write_events()
        # print('>>> %s TERMINATED:' % (self.baseObject.pe.id))
        # print(self.events)
        # print(self.baseObject.pe.events)
        # print self
        # print('>>> %s TERMINATED AT %s' % (self.baseObject.pe.id, t.end))

    def write_events():
        None
