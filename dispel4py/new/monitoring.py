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
        if self.times_count:
            self.log('Average processing time: %s' % (self.times_total /
                                                      self.times_count))


class EventTimestamp(object):

    def __init__(self, name, events):
        self.name = name
        self.data = {}
        self.events = events

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
        self.events.append(self)

from dispel4py.utils import total_size


class TimestampEventsPE(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self._monitoring_events = []

    # def write(self, name, data):
    #     with EventTimestamp('write') as t:
    #         t.data['output'] = name
    #         t.data['size'] = total_size(data)
    #         self._monitoring_events.append(t)
    #         return self.baseObject.write(name, data)

    def preprocess(self):
        with EventTimestamp('preprocess', self._monitoring_events) as t:
            try:
                return self.baseObject.preprocess()
            except Exception as exc:
                t.data['error'] = repr(exc)
                raise

    def process(self, inputs):
        with EventTimestamp('process', self._monitoring_events) as t:
            try:
                return self.baseObject.process(inputs)
            except Exception as exc:
                t.data['error'] = repr(exc)
                raise

    def postprocess(self):
        with EventTimestamp('postprocess', self._monitoring_events) as t:
            try:
                return self.baseObject.postprocess()
            except Exception as exc:
                t.data['error'] = repr(exc)
                raise


class TimestampEventsWrapper(MonitoringWrapper):

    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.events = []
        self.baseObject.pe = TimestampEventsPE(self.baseObject.pe)
        self.data_count = defaultdict(int)

    # def process(self):
    #     with EventTimestamp('total_process') as t:
    #         self.events.append(t)
    #         try:
    #             self.baseObject.process()
    #         except:
    #             t.error = traceback.format_exc(1)
    #             raise

    def _write(self, name, data):
        with EventTimestamp('write', self.events) as t:
            t.data['output'] = name
            t.data['size'] = total_size(data)
            self.data_count[name] += 1
            data_id = (self.baseObject.pe.id,
                       self.baseObject.pe.rank,
                       name,
                       self.data_count[name])
            t.data['id'] = data_id
            annotated = {'data': data, '_d_id': data_id}
            self.baseObject._write(name, annotated)
            # self.baseObject._write(name, data)
        # print('>>> %s WRITE: %.6f s to %.6f s' %
        #       (self.baseObject.pe.id, t.start, t.end))
        self.write_events()

    def _read(self):
        with EventTimestamp('read', self.events) as t:
            obj = self.baseObject._read()
            try:
                data, status = obj
                original = {}
                t.data['input'] = []
                for input_name, input_data in data.items():
                    t.data['input'].append((input_name, input_data['_d_id']))
                    original[input_name] = input_data['data']
                obj = original, status
            except:
                # import traceback
                # print(traceback.format_exc())
                # if the data is not a dictionary (could be None)
                pass
        self.write_events()
        # print('>>> %s READ: %.6f s to %.6f s' %
        #       (self.baseObject.pe.id, t.start, t.end))
        return obj

    def _terminate(self):
        with EventTimestamp('terminate', self.events):
            self.baseObject._terminate()
        self.write_events()
        # print('>>> %s TERMINATED:' % (self.baseObject.pe.id))
        # print(self.events)
        # print(self.baseObject.pe.events)
        # print self
        # print('>>> %s TERMINATED AT %s' % (self.baseObject.pe.id, t.end))

    def write_events():
        None


import sys
import multiprocessing
from dispel4py.new.processor import STATUS_TERMINATED
from dispel4py.workflow_graph import drawDot


def write_info_file(job_dir, info):
    for proc, inputs in info['inputs'].items():
        new_inputs = {}
        for name, inp_procs in inputs.items():
            try:
                p, i = name
                new_inputs['%s_%s' % (p, i)] = inp_procs
            except:
                new_inputs[name] = inp_procs
        info['inputs'][proc] = new_inputs
    try:
        for proc, outputs in info['outputs'].items():
            new_outputs = {}
            for name, outp in outputs.items():
                try:
                    p, o = name
                    new_name = '%s_%s' % (p, o)
                except:
                    new_name = name
                for target in outp:
                    new_outputs[new_name] = target[1].destinations
            info['outputs'][proc] = new_outputs
    except:
        pass
    with open(job_dir + '/info', 'w') as f:
        f.write(json.dumps(info))


def write_graph_vis(job_dir, workflow, info):
    try:
        with open(os.path.join(job_dir, 'graph.png'), 'w') as f:
            f.write(drawDot(workflow))
        info['graph'] = True
    except:
        import traceback
        print(traceback.format_exc())
        pass


def publish_and_subscribe(
        workflow, info, monitoring_queue, monitoring_outputs):
    subscriptions = []
    subs_processes = []
    if info['name'] is None:
        info['name'] = str(uuid.uuid4())
    info['start_time'] = format_timestamp(datetime.now())
    try:
        job_dir = os.path.join(ROOT_DIR, info['name'])
        os.makedirs(job_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(job_dir):
            pass
        else:
            raise
    write_graph_vis(job_dir, workflow, info)
    for m in monitoring_outputs:
        subs = multiprocessing.Queue()
        args = [subs, info]
        command = m.split(' ')
        method = command[0]
        if len(command) > 1:
            args += command[1:]
        subscriptions.append(subs)
        p = multiprocessing.Process(target=globals()[method], args=args)
        subs_processes.append(p)
        p.start()
    publisher = multiprocessing.Process(
        target=publish,
        args=(monitoring_queue, subscriptions, info, ))
    publisher.start()
    return publisher, subs_processes


def publish(queue, subscriptions, info):
    job_dir = os.path.join(ROOT_DIR, info['name'])
    write_info_file(job_dir, info)
    try:
        for item in iter(queue.get, STATUS_TERMINATED):
            for s in subscriptions:
                s.put(item)
    finally:
        info['end_time'] = format_timestamp(datetime.now())
        write_info_file(job_dir, info)
        for s in subscriptions:
            s.put(STATUS_TERMINATED)


import json
import os
import errno
import uuid
import traceback

ROOT_DIR = os.path.expanduser('~') + '/.dispel4py/monitoring'


def write_stdout(input_queue, info):
    '''
    Writes all monitoring information to stdout.
    '''
    for item in iter(input_queue.get, STATUS_TERMINATED):
        pe_id, rank, event = item
        info = [pe_id, rank, event.name, event.data, event.start, event.end]
        print(','.join(str(x) for x in info))


def write_file(input_queue, info, file_name):
    '''
    Writes all monitoring information to the given file.
    :file_name: name of the output file
    '''
    try:
        with open(file_name, 'w') as f:
            for item in iter(input_queue.get, STATUS_TERMINATED):
                pe_id, rank, event = item
                record = [pe_id, rank,
                          event.name, event.data, event.start, event.end]
                f.write(','.join(str(x) for x in record) + '\n')
    except Exception as exc:
        sys.stderr.write(
            'WARNING: Failed to write monitoring information to %s: %s\n' %
            (file_name, exc))


def write_timeline(input_queue, info):
    '''
    Writes all monitoring information to the given file.
    :job: name of the output file, or None to generate a job ID
    '''
    job_dir = os.path.join(ROOT_DIR, info['name'])
    try:
        os.makedirs(job_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(job_dir):
            pass
        else:
            raise

    try:
        with open(os.path.join(job_dir, 'timestamps'), 'w') as f:
            for item in iter(input_queue.get, STATUS_TERMINATED):
                pe_id, rank, event = item
                timestamp = {'content': event.name,
                             'start': event.start,
                             'end': event.end,
                             'group': rank}
                f.write(json.dumps(timestamp))
                f.write('\n')
    except Exception:
        import traceback
        print(traceback.format_exc())


from collections import defaultdict
import bisect


def collect_timestamps(input_queue, collection):
    for item in iter(input_queue.get, STATUS_TERMINATED):
        add_to_collection(item, collection)


def add_to_collection(item, info):
    collection = info['status']
    pe_id, rank, event = item
    if pe_id not in collection:
        collection[pe_id] = {
            'detail': {},
            'summary': {'count': 0, 'time': 0.0,
                        'error_count': 0,
                        'processes': []}
        }
    if rank not in collection[pe_id]['detail']:
        collection[pe_id]['detail'][rank] = {
            'write': {}, 'read': {},
            'process': defaultdict(float),
        }
        bisect.insort_left(collection[pe_id]['summary']['processes'], rank)
    if rank not in info['processes']:
        bisect.insort_left(info['processes'], rank)
    pe_data = collection[pe_id]['detail'][rank]
    pe_total = collection[pe_id]['summary']

    if event.name == 'write':
        writes = pe_data['write']
        output_name = event.data['output']
        info['total_write_size'] += event.data['size']
        info['total_write_count'] += 1
        if output_name in writes:
            writes[output_name]['count'] += 1
            writes[output_name]['size'] += event.data['size']
        else:
            writes[output_name] = {'count': 1, 'size': event.data['size']}
    elif event.name == 'read':
        reads = pe_data['read']
        if 'input' in event.data:
            input_names = event.data['input']
            for input_name in input_names:
                info['total_read_count'] += 1
                if input_name in reads:
                    reads[input_name]['count'] += 1
                    # reads[input_name]['size'] += event.data['size']
                else:
                    reads[input_name] = {'count': 1}
                    # reads[input_name]['size'] = event.data['size']
    elif event.name == 'process':
        procs = pe_data['process']
        t_proc = event.end - event.start
        info['total_time'] += t_proc
        procs['time'] += t_proc
        procs['count'] += 1
        if 'error' in event.data:
            procs['error_count'] += 1
            procs['last_error'] = event.data['error']
            pe_total['error_count'] += 1
            pe_total['last_error'] = event.data['error']
        pe_total['time'] += t_proc
        pe_total['count'] += 1


def create_monitoring_info():
    return {'total_write_size': 0,
            'total_write_count': 0,
            'total_read_count': 0,
            'total_time': 0,
            'processes': [],
            'status': {}}


def print_stack(input_queue):
    collection = create_monitoring_info()
    try:
        collect_timestamps(input_queue, collection)
    finally:
        print(collection)


from datetime import datetime


def format_timestamp(tst):
    return tst.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def publish_stack(input_queue, info):
    job = info['name']
    job_dir = os.path.join(ROOT_DIR, job)
    collection = create_monitoring_info()
    counter = 0
    print('Monitoring job %s' % job)
    collection['name'] = job
    collection['start_time'] = info['start_time']
    tst = time.time()
    try:
        for item in iter(input_queue.get, STATUS_TERMINATED):
            counter += 1
            add_to_collection(item, collection)
            if time.time() - tst > 1 or counter > 10000:
                with open(os.path.join(job_dir, 'stack'), 'w') as f:
                    f.write(json.dumps(collection))
                counter = 0
                tst = time.time()

    finally:
        endtime = format_timestamp(datetime.now())
        collection['end_time'] = endtime
        with open(os.path.join(job_dir, 'stack'), 'w') as f:
            f.write(json.dumps(collection))


# *********** MongoDB *************


def store(input_queue, info,
          mongodb_url='mongodb://localhost:27017/',
          mongodb_database='dispel4py_monitor',
          mongodb_info='job_info',
          mongodb_collection='raw'):
    '''
    Store monitoring data in MongoDB.
    '''
    from pymongo import MongoClient
    client = MongoClient(mongodb_url)
    db = client[mongodb_database]
    info_col = db[mongodb_info]
    collection = db[mongodb_collection]
    if mongodb_info not in db.collection_names():
        info_col.create_index('name', background=True)
    if mongodb_collection not in db.collection_names():
        collection.create_index('job', background=True)
        collection.create_index([('job', 1), ('data.id', 1)], background=True)

    del_result = info_col.delete_many({'name': info['name']})
    collection.delete_many({'job': info['name']})
    if del_result.deleted_count:
        print('Monitor: WARNING: replacing existing job "%s"' % info['name'])

    info_record = {
        'name': info['name'],
        'mapping': info['mapping'],
        'processes': info['processes'],
        'start_time': info['start_time']
    }
    info_record['inputs'] = \
        {str(proc):
            [{'name': name, 'sources': sources}
                for name, sources in inputs.items()]
            for proc, inputs in info['inputs'].items()}
    info_record['outputs'] = {}
    for proc, outputs in info['outputs'].items():
        info_record['outputs'][str(proc)] = []
        for name, outp in outputs.items():
            info_record['outputs'][str(proc)].append({
                'name': name,
                'destinations': [target[1].destinations for target in outp]})
    try:
        info_record['graph'] = info['graph']
    except:
        pass
    info_col.insert_one(info_record)

    try:
        for item in iter(input_queue.get, STATUS_TERMINATED):
            try:
                pe_id, rank, event = item
                record = {'job': info['name'],
                          'pe': pe_id,
                          'process': rank,
                          'name': event.name,
                          'data': event.data,
                          'time': event.end - event.start,
                          'start': event.start,
                          'end': event.end}
                collection.insert_one(record)
            except:
                print(traceback.format_exc())
    finally:
        endtime = format_timestamp(datetime.now())
        info_col.find_one_and_update(
            {'_id': info_record['_id']},
            {'$set': {'end_time': endtime}}
        )
