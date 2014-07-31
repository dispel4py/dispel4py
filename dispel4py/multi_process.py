# Copyright (c) The University of Edinburgh 2014
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

import copy
import json
import multiprocessing
import sys
import time
import types
import traceback

from dispel4py import simple_process, workflow_graph
from dispel4py.GenericPE import GROUPING
from dispel4py.utils import make_hash

# delay in seconds between requests for input data from queue
POLL_DELAY = 1

class QueueWriter(object):
    def __init__(self, targets):
        self.targets = targets
    def write(self, result):
        if result:
            for dest_input, communication in self.targets:
                output = { dest_input : result }
                destinations = communication.getDestination(output)
                _distributionWrapper(output, destinations)

class NullWriter(object): 
    def write(self, result):
        None


def _log(self, msg):
    print("%s (rank %s): %s" % (self.id, self.rank, msg))

def _processWorker(pe, staticinputs, inconnections, outconnections):
    if inconnections:
        receive = _receiveWrapper
    else:
        receive = lambda rank, inconnections: staticinputs.pop(0) if staticinputs else None
    pe.log('Preprocessing')
    pe.preprocess()
    count = 0
    pe.log('Starting to process')
    open_inconnections = dict(inconnections)
    while True:
        # pe.log('Waiting for input data %s' % inconnections)
        inputs = receive(pe.rank, open_inconnections)
        # pe.log('Received data %s' % inputs)
        if inputs is None:
            pe.log('Processing is complete. Processed %s block(s).' % count)
            # we're finished
            break
        count += 1
        results = pe.process(inputs)
        if results is not None:
            for name, value in results.iteritems():
                # pe.log('Writing %s to %s.%s' % ({name:value}, pe.id, name))
                try:
                    for dest_input, communication in outconnections[name]:
                        output = { dest_input : value }
                        destinations = communication.getDestination(output)
                        _distributionWrapper(output, destinations )
                except KeyError:
                    # the output is not connected so we ignore it
                    # print 'Discarding output from %s.%s' % (pe.id, name)
                    pass
                    
    pe.log('Closing all output queues')
    for name in outconnections:
        for dest_input, communication in outconnections[name]:
            communication.closeQueue(pe.rank)
            # for rank, queue in communication.getQueues().iteritems():
            #     queue.put(None)
    
    pe.log('Postprocessing')
    pe.postprocess()
    pe.log('Done.')
    
class Communication(object):
    def __init__(self, source_pes, dest_pes):
        self.source_pes = source_pes
        self.dest_pes = dest_pes
        self.closed_queues = multiprocessing.Value('i', len(source_pes))
    def closeQueue(self, rank):
        with self.closed_queues.get_lock():
            self.closed_queues.value -= 1
    def isClosed(self):
        return self.closed_queues.value <= 0
    
class ShuffleCommunication(Communication):
    def __init__(self, source_pes, dest_pes):
        Communication.__init__(self, source_pes, dest_pes)
        self.queue = multiprocessing.Queue()
        self.queues = { rank : self.queue for rank in self.dest_pes }
    def getDestination(self, data):
        return [self.queue]
    def getQueues(self):
        return self.queues

class GroupByCommunication(Communication):
    def __init__(self, source_pes, dest_pes, input_name, groupby):
        Communication.__init__(self, source_pes, dest_pes)
        self.groupby = groupby
        self.connections = { rank : multiprocessing.Queue() for rank in dest_pes }
        self.destinations = [ q for r, q in self.connections.iteritems() ]
        self.input_name = input_name
    def getDestination(self, data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        index = abs(make_hash(output))%len(self.connections)
        return [self.destinations[index]]
    def getQueues(self):
        return self.connections
        
                        
def _receiveWrapper(rank, open_inconnections):
    inputs = {}
    closed_inconnections = set()
    while not inputs:
        for name, comm in open_inconnections.iteritems():
            try:
                queue = comm.getQueues()[rank]
                received = queue.get_nowait()
                # print 'Received %s from %s' % (received, queue)
                inputs[name] = received[name]
                break
            except multiprocessing.queues.Empty:
                # if there is nothing in the queue we check if the input is closed
                if comm.isClosed():
                    # print 'input %s is closed' % name
                    closed_inconnections.add(name)
                    continue
        for name in closed_inconnections:
            del open_inconnections[name]
        if not inputs:
            # if everything has been closed we stop
            if not open_inconnections:
                return None
            # otherwise wait and try again with the remaining queues
            time.sleep(POLL_DELAY)
    # print 'closed inputs: %s' % closed_inputs
    return inputs

def _distributionWrapper(data, outconnections):
    for queue in outconnections:
        # print 'WRITING %s to %s' % (data, queue)
        queue.put(data)
                
def _getCommunication(dest, dest_input, source_pes, dest_pes):
    communication = ShuffleCommunication(source_pes, dest_pes)
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                communication = GroupByCommunication(source_pes, dest_pes, dest_input, groupingtype)
            # elif groupingtype == 'all':
            #     communication = OneToAllCommunication(connections)
            # elif groupingtype == 'global':
            #     communication = AllToOneCommunication(connections)
    except KeyError:
        print("No input '%s' defined for PE '%s'" % (dest_input, dest.id))
        raise
    return communication
   

def multiprocess(workflow, numProcesses, inputs=[{}]):
    '''
    Executes the given inputs in the the graph in multiple processes.
    If the graph is partitioned, i.e. every node has been assigned to a partition by giving a value
    to pe.partition, each partition is executed in a separate process. 
    If the graph is not partitioned each connected component is executed in a separate process.
    Results of each partition are communicated to the main process and returned by the method.
    '''
    
    success, sources, processes = _assign(workflow, numProcesses)
    if success:
        print 'Processes: %s' %  { pe.id : rank for (pe, rank) in processes.iteritems() }
    else:
        print 'Simple processing: not implemented yet.'
        return
        
    
    graph = workflow.graph
    inconnections = {}
    outconnections = {}
    process_pes = {}
    for node in graph.nodes():
        pe = node.getContainedObject()
        inconnections[pe] = {}
        outconnections[pe] = {}
        for output_name in pe.outputconnections:
            pe.outputconnections[output_name]['writer']=NullWriter()
        process_pes[pe] = {}
        for proc in processes[pe]:
            cp = copy.deepcopy(pe)
            cp.rank = proc
            cp.log = types.MethodType(_log, cp)
            process_pes[pe][proc] = cp
        
    # print 'PROCESS PES: %s' % process_pes
    mappedInputs = {}
    
    for node in graph.nodes():
        pe = node.getContainedObject()
        outconnections[pe] = {}
        for edge in graph.edges(node, data=True):
            direction = edge[2]['DIRECTION']
            source = direction[0]
            source_output = edge[2]['FROM_CONNECTION']
            dest = direction[1]
            dest_processes=processes[dest]
            source_processes=processes[source]
            if dest == pe:
                dest_input = edge[2]['TO_CONNECTION']
            if source == pe:
                allconnections = edge[2]['ALL_CONNECTIONS']
                try:
                    inconx = inconnections[dest]
                except KeyError:
                    inconx = {}
                    inconnections[dest] = inconx
                outconnections[pe][source_output] = []
                for (source_output, dest_input) in allconnections:
                    communication = _getCommunication(dest, dest_input, process_pes[source], process_pes[dest])
                    outconnections[pe][source_output].append((dest_input, communication))
                    inconx[dest_input] = communication
                for cp in process_pes[pe].values():
                    cp.outputconnections[source_output]['writer']=QueueWriter(outconnections[pe][source_output])

        mappedInputs[pe] = inputs if pe in sources else None
    # print 'INCONNECTIONS: %s' % inconnections
    # print 'OUTCONNECTIONS: %s' % outconnections
    # print 'MAPPED INPUTS: %s' % mappedInputs
    
    jobs = []
    for node in graph.nodes():
        pe = node.getContainedObject()
        for cp in process_pes[pe].values():
            p = multiprocessing.Process(target=_processWorker, args=(cp, mappedInputs[pe], inconnections[pe], outconnections[pe]))
            jobs.append(p)
        # print p

    for j in jobs:
        j.start()
        
    for j in jobs:
        j.join()
        
def _getNumProcesses(numSources, numProcesses, totalProcesses, size):
    div = max(1, totalProcesses-numSources)
    return int(numProcesses * (size - numSources)/div)  

def _assign(workflow, size):
    graph = workflow.graph
    processes={}
    success=True
    totalProcesses = 0
    numSources = 0
    sources = []
    for node in graph.nodes():
        pe=node.getContainedObject()
        if workflow_graph.getConnectedInputs(node, graph):
            totalProcesses = totalProcesses + pe.numprocesses
        else:
            sources.append(pe)
            totalProcesses += 1
            numSources += 1
    
    if totalProcesses > size:
        # we need at least one process for each node in the graph
        success = False
    else:    
        node_counter = 0
        for node in graph.nodes():
            pe=node.getContainedObject()
            prcs = 1 if pe in sources else _getNumProcesses(numSources, pe.numprocesses, totalProcesses, size)
            processes[pe]=range(node_counter, node_counter+prcs)
            node_counter = node_counter + prcs
    return success, sources, processes

if __name__ == "__main__":
    import argparse
    from dispel4py.utils import loadGraph
    
    parser = argparse.ArgumentParser(description='Processing of a dispel4py graph using multiple processes.')
    parser.add_argument('module', help='module that creates a dispel4py graph')
    parser.add_argument('-n', '--num', metavar='num_processes', required=True, type=int, help='number of processes to run')
    parser.add_argument('-a', '--attr', metavar='attribute', help='name of graph variable in the module')
    parser.add_argument('-f', '--file', metavar='inputfile', help='file containing the input dataset in JSON format')
    parser.add_argument('-i', '--iter', metavar='iterations', type=int, help='number of iterations')
    args = parser.parse_args()
    
    graph = loadGraph(args.module, args.attr)
    graph.flatten()
    
    # run only once if no input data
    inputs = [{}]
    if args.file:
        try:
            with open(args.file) as inputfile:
                inputs = json.loads(inputfile.read())
            if not type(inputs) == list:
                inputs = [inputs]
            print "Processing input file %s" % args.file
        except:
            print traceback.format_exc()
            print 'Failed to read input file %s' % args.file
            sys.exit(1)
    elif args.iter:
        inputs = [ {} for i in range(args.iter) ]
        print "Processing %s iteration(s)" % args.iter
    else:
        print 'Processing 1 iteration.'
    
    print 'Starting multiprocessing with %s processes.' % args.num    
    print 'Inputs: %s' % inputs
    multiprocess(graph, args.num, inputs)
