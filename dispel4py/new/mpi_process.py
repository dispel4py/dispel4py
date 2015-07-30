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

'''
Enactment of dispel4py graphs with MPI.

From the commandline, run the following command::

    dispel4py mpi <module> [-h] [-a attribute] [-f inputfile] [-i iterations]

with parameters

:module:    module that creates a Dispel4Py graph
:-a attr:   name of the graph attribute within the module (optional)
:-f file:   file containing input data in JSON format (optional)
:-i iter:   number of iterations to compute (default is 1)
:-h:        print this help page

For example::

    mpiexec -n 6 dispel4py mpi dispel4py.examples.graph_testing.pipeline_test\
        -i 5
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processing 5 iterations.
    Processes: {'TestProducer0': [5], 'TestOneInOneOut5': [2],\
        'TestOneInOneOut4': [4], 'TestOneInOneOut3': [3],\
        'TestOneInOneOut2': [1], 'TestOneInOneOut1': [0]}
    TestOneInOneOut1 (rank 0): Processed 5 iterations.
    TestOneInOneOut2 (rank 1): Processed 5 iterations.
    TestOneInOneOut3 (rank 3): Processed 5 iterations.
    TestProducer0 (rank 5): Processed 5 iterations.
    TestOneInOneOut4 (rank 4): Processed 5 iterations.
    TestOneInOneOut5 (rank 2): Processed 5 iterations.
'''

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

from dispel4py.new.processor\
    import GenericWrapper, simpleLogger, STATUS_TERMINATED, STATUS_ACTIVE
from dispel4py.new import processor

import argparse
import sys
import types
import traceback


def mpi_excepthook(type, value, trace):
    '''
    Sending abort to all processes if an exception is raised.
    '''
    if rank == 0:
        traceback.print_tb(trace)
    comm.Abort(1)


sys.excepthook = mpi_excepthook


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph to MPI processes.')
    parser.add_argument('-s', '--simple', help='force simple processing',
                        action='store_true')
    parser.add_argument('--monitoring', nargs='?', action='append',
                        help='monitor processing and write timestamps to file')
    parser.add_argument('--name', help='job name')
    result = parser.parse_args(args, namespace)
    return result


def process(workflow, inputs, args):
    processes = {}
    inputmappings = {}
    outputmappings = {}
    success = True
    nodes = [node.getContainedObject() for node in workflow.graph.nodes()]
    process_size = size
    if args.monitoring:
        # assign one additional process to the monitor
        process_size -= 1
    if rank == 0 and not args.simple:
        try:
            processes, inputmappings, outputmappings =\
                processor.assign_and_connect(workflow, process_size)
        except:
            success = False
    success = comm.bcast(success, root=0)

    if args.simple or not success:
        ubergraph = processor.create_partitioned(workflow)
        nodes = [node.getContainedObject() for node in ubergraph.graph.nodes()]
        if rank == 0:
            print('Partitions: %s' % ', '.join(('[%s]' % ', '.join(
                (pe.id for pe in part)) for part in workflow.partitions)))
            for node in ubergraph.graph.nodes():
                wrapperPE = node.getContainedObject()
                print('%s contains %s' % (wrapperPE.id,
                                          [n.getContainedObject().id for n in
                                           wrapperPE.workflow.graph.nodes()]))
            try:
                processes, inputmappings, outputmappings =\
                    processor.assign_and_connect(ubergraph, process_size)
                inputs = processor.map_inputs_to_partitions(ubergraph, inputs)
                success = True
            except:
                # print traceback.format_exc()
                print('dispel4py.mpi_process: \
                    Not enough processes for execution of graph')
                success = False

    success = comm.bcast(success, root=0)

    if not success:
        return

    monitoring_rank = None
    monitoring_outputs = None
    monitoring_job_name = None
    try:
        if rank == 0 and args.monitoring:
            all_processes = []
            for pe, procs in processes.items():
                all_processes.extend(procs)
            monitoring_rank = len(all_processes)
            monitoring_outputs = list(args.monitoring)
            monitoring_job_name = args.name
    except AttributeError:
        pass

    try:
        inputs = {pe.id: v for pe, v in inputs.items()}
    except AttributeError:
        pass
    processes = comm.bcast(processes, root=0)
    inputmappings = comm.bcast(inputmappings, root=0)
    outputmappings = comm.bcast(outputmappings, root=0)
    inputs = comm.bcast(inputs, root=0)

    monitoring_rank = comm.bcast(monitoring_rank, root=0)
    monitoring_outputs = comm.bcast(monitoring_outputs, root=0)
    monitoring_job_name = comm.bcast(monitoring_job_name, root=0)

    if rank == 0:
        print('Processes: %s' % processes)
        # print 'Inputs: %s' % inputs

    if monitoring_rank >= size:
        if rank == 0:
            print('dispel4py monitoring: Not enough processes. \
Please allow one additional process for the collection of monitoring data.')
        return
    if rank == monitoring_rank:
        collect_monitoring_info(workflow,
                                monitoring_job_name, monitoring_outputs,
                                processes, inputmappings, outputmappings)

    for pe in nodes:
        if rank in processes[pe.id]:
            try:
                provided_inputs = processor.get_inputs(pe, inputs)
                wrapper = MPIWrapper(pe, provided_inputs)
                wrapper.targets = outputmappings[rank]
                wrapper.sources = inputmappings[rank]
                if monitoring_rank:
                    wrapper = add_monitoring_wrapper(wrapper, monitoring_rank)
                wrapper.process()
            finally:
                if monitoring_rank:
                    comm.isend(
                        None,
                        tag=STATUS_TERMINATED,
                        dest=monitoring_rank)


def collect_monitoring_info(workflow, monitoring_job_name, monitoring_outputs,
                            processes, inputmappings, outputmappings):
        from dispel4py.new.monitoring import publish_and_subscribe
        import multiprocessing

        info = {'name': monitoring_job_name,
                'processes': processes,
                'inputs': inputmappings,
                'outputs': outputmappings,
                'mapping': 'mpi'}
        monitoring_queue = multiprocessing.Queue()
        publisher, subscription_procs = \
            publish_and_subscribe(
                workflow, info, monitoring_queue, monitoring_outputs)
        num_terminated = 0
        while num_terminated < rank:
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG,
                            status=status)
            tag = status.Get_tag()
            if tag == STATUS_TERMINATED:
                num_terminated += 1
            else:
                monitoring_queue.put(msg)
        monitoring_queue.put(STATUS_TERMINATED)


def write_events(wrapper):
    while wrapper.events:
        event = wrapper.events.pop(0)
        try:
            # print('writing event: %s' % event)
            request = comm.isend(
                (wrapper.baseObject.pe.id,
                 wrapper.baseObject.pe.rank,
                 event),
                dest=wrapper.monitoring_rank)
            status = MPI.Status()
            request.Wait(status)
        except:
            wrapper.baseObject.pe.log(
                'Failed to send monitoring info to rank %s: %s'
                % (wrapper.monitoring_rank, traceback.format_exc()))
    while wrapper.baseObject.pe._monitoring_events:
        event = wrapper.baseObject.pe._monitoring_events.pop(0)
        try:
            # print('writing event: %s' % event)
            request = comm.isend(
                (wrapper.baseObject.pe.id,
                 wrapper.baseObject.pe.rank,
                 event),
                dest=wrapper.monitoring_rank)
            status = MPI.Status()
            request.Wait(status)
        except:
            wrapper.baseObject.pe.log(
                'Failed to send monitoring info to rank %s: %s'
                % (wrapper.monitoring_rank, traceback.format_exc()))


from dispel4py.new.monitoring import TimestampEventsWrapper


def add_monitoring_wrapper(wrapper, monitoring_rank,
                           WrapperMonitor=TimestampEventsWrapper):
    wrapper.monitoring_rank = monitoring_rank
    wrapper.write_events = types.MethodType(write_events, wrapper)
    monitor = WrapperMonitor(wrapper)
    # now point the PE output writers to the new monitoring wrapper
    # otherwise any output written with self.write() is not captured
    for output in wrapper.pe.outputconnections.values():
        output['writer'].wrapper = monitor
    return monitor


class MPIWrapper(GenericWrapper):

    def __init__(self, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.provided_inputs = provided_inputs
        self.terminated = 0

    def _read(self):
        result = super(MPIWrapper, self)._read()
        if result is not None:
            return result

        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        while tag == STATUS_TERMINATED:
            self.terminated += 1
            if self.terminated >= self._num_sources:
                break
            else:
                msg = comm.recv(source=MPI.ANY_SOURCE,
                                tag=MPI.ANY_TAG,
                                status=status)
                tag = status.Get_tag()
        return msg, tag

    def _write(self, name, data):
        try:
            targets = self.targets[name]
        except KeyError:
            # no targets
            # self.pe.log('Produced output: %s' % {name: data})
            return
        for (inputName, communication) in targets:
            output = {inputName: data}
            dest = communication.getDestination(output)
            for i in dest:
                try:
                    # self.pe.log('Sending %s to %s' % (output, i))
                    request = comm.isend(output, tag=STATUS_ACTIVE, dest=i)
                    status = MPI.Status()
                    request.Wait(status)
                except:
                    self.pe.log(
                        'Failed to send data stream "%s" to rank %s: %s'
                        % (name, i, traceback.format_exc()))

    def _terminate(self):
        for output, targets in self.targets.items():
            for (inputName, communication) in targets:
                for i in communication.destinations:
                    # self.pe.log('Terminating consumer %s' % i)
                    comm.isend(None, tag=STATUS_TERMINATED, dest=i)


def main():
    from dispel4py.new.processor \
        import load_graph_and_inputs, parse_common_args

    args, remaining = parse_common_args()
    try:
        args = parse_args(remaining, args)
    except SystemExit:
        raise

    graph, inputs = load_graph_and_inputs(args)
    if graph is not None:
        errormsg = process(graph, inputs, args)
        if errormsg:
            print(errormsg)
