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

'''
Enactment of dispel4py graphs using multiprocessing.
'''

import copy
import json
import multiprocessing
import sys
import time
import types
import traceback

from dispel4py import simple_process, workflow_graph
from dispel4py.core import GROUPING
from dispel4py.utils import make_hash

class QueueWriter(object):
    def __init__(self, targets):
        self.targets = targets
    def write(self, result):
        if result:
            for dest_input, communication in self.targets:
                output = { dest_input : result }
                communication.send(output)
                # destinations = communication.getDestination(output)
                # _distributionWrapper(output, destinations)

class NullWriter(object): 
    def write(self, result):
        None


def _log(self, msg):
    print("%s (rank %s): %s" % (self.id, self.rank, msg))

def _processWorker(pe, staticinputs, inputqueue, numInputs, outconnections):
    if numInputs:
        # pe.log("I'm a bolt - number of inputs: %s" % numInputs)
        receive = _receiveWrapper
        open_inputs = numInputs
    else:
        # pe.log("I'm a spout")
        receive = lambda inputqueue: staticinputs.pop(0) if staticinputs else None
        open_inputs = 1
    pe.log('Preprocessing')
    pe.preprocess()
    count = 0
    pe.log('Starting to process')
    while open_inputs > 0:
        # pe.log('Waiting for input data from %s' % inputqueue.name)
        inputs = receive(inputqueue)
        # pe.log('Received data %s' % inputs)
        if inputs is None:
            open_inputs -= 1
            continue
        count += 1
        results = pe.process(inputs)
        if results is not None:
            for name, value in results.iteritems():
                # pe.log('Writing %s to %s.%s' % ({name:value}, pe.id, name))
                try:
                    for dest_input, communication in outconnections[name]:
                        output = { dest_input : value }
                        communication.send(output)
                except KeyError:
                    # the output is not connected so we ignore it
                    # print 'Discarding output from %s.%s' % (pe.id, name)
                    pass
                    
    pe.log('Processing is complete. Processed %s block(s).' % count)
    for name in outconnections:
        for dest_input, communication in outconnections[name]:
            communication.closeQueue()
            # for rank, queue in communication.getQueues().iteritems():
            #     queue.put(None)
    
    pe.log('Postprocessing')
    pe.postprocess()
    pe.log('Done.')
    
class Communication(object):
    def __init__(self, source_pes, dest_pes, dest_queues):
        self.source_pes = source_pes
        self.dest_pes = dest_pes
        self.dest_queues = dest_queues
    def closeQueue(self):
        for q in self.destinations:
            q.put(None)
    def send(self, data):
        dest = self.getDestination(data)
        for q in dest:
            q.put(data)
    
class ShuffleCommunication(Communication):
    def __init__(self, rank, source_pes, dest_pes, dest_queues):
        Communication.__init__(self, source_pes, dest_pes, dest_queues)
        self.currentIndex = (source_pes.keys().index(rank) % len(dest_pes)) -1
        self.destinations = [ dest_queues[p] for r, p in dest_pes.iteritems() ]
        # print 'shuffle sources: %s, destinations: %s' % (source_pes.keys(), [ r for r in dest_pes ])
        # print 'shuffle, rank %s, currentIndex: %s' % (rank, self.currentIndex)
    def getDestination(self, data):
        self.currentIndex = (self.currentIndex+1)%len(self.destinations)
        # print 'sending %s to %s' % (data, self.dest_pes.keys()[index])
        return [self.destinations[self.currentIndex]]

class GroupByCommunication(Communication):
    def __init__(self, source_pes, dest_pes, dest_queues, input_name, groupby):
        Communication.__init__(self, source_pes, dest_pes, dest_queues)
        self.groupby = groupby
        self.destinations = [ dest_queues[p] for r, p in dest_pes.iteritems() ]
        self.input_name = input_name
    def getDestination(self, data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        index = abs(make_hash(output))%len(self.dest_pes)
        return [self.destinations[index]]
        
                        
def _receiveWrapper(queue):
    received = queue.get()
    return received

def _distributionWrapper(data, outconnections):
    # print 'WRITING %s to %s' % (data, queue)
    comm.send(data)
                
def _getCommunication(source, dest, dest_input, process_pes, input_queues):
    communication = {}
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                for rank, cp in process_pes[source].iteritems():
                    communication[cp] = GroupByCommunication(process_pes[source], process_pes[dest], input_queues, dest_input, groupingtype)
            # elif groupingtype == 'all':
            #     communication = OneToAllCommunication(connections)
            # elif groupingtype == 'global':
            #     communication = AllToOneCommunication(connections)
        else:
            for rank, cp in process_pes[source].iteritems():
                communication[cp] = ShuffleCommunication(rank, process_pes[source], process_pes[dest], input_queues)
    except KeyError:
        print("No input '%s' defined for PE '%s'" % (dest_input, dest.id))
        raise
    return communication
   
from dispel4py.partition import simpleProcess

def multiprocess(workflow, numProcesses, inputs=[{}], simple=False):
    '''
    Executes the given inputs in the the graph in multiple processes.
    If the graph is partitioned, i.e. every node has been assigned to a partition by giving a value
    to pe.partition, each partition is executed in a separate process. 
    If the graph is not partitioned each connected component is executed in a separate process.
    Results of each partition are communicated to the main process and returned by the method.
    '''
    
    success, sources, processes = _assign(workflow, numProcesses)
    if success and not simple:
        graph = workflow.graph
    else:
        print 'Start simple processing.'
        uberWorkflow, inputs = simpleProcess(workflow, sources, inputs)
        success, sources, processes = _assign(uberWorkflow, numProcesses)
        graph = uberWorkflow.graph
        for node in uberWorkflow.graph.nodes():
            wrapperPE = node.getContainedObject()
            print('%s contains %s' % (wrapperPE.id, [n.getContainedObject().id for n in wrapperPE.workflow.graph.nodes()]))
        
    print 'Processes: %s' %  { pe.id : rank for (pe, rank) in processes.iteritems() }
    outconnections = {}
    process_pes = {}
    input_queues = {}
    numInputs = {}
    for node in graph.nodes():
        pe = node.getContainedObject()
        numInputs[pe] = 0
        outconnections[pe] = {}
        for output_name in pe.outputconnections:
            pe.outputconnections[output_name]['writer']=NullWriter()
        process_pes[pe] = {}
        for proc in processes[pe]:
            cp = copy.deepcopy(pe)
            cp.rank = proc
            cp.log = types.MethodType(_log, cp)
            process_pes[pe][proc] = cp
            q= multiprocessing.Queue()
            q.name = 'Queue_%s_%s' % (cp.id, cp.rank)
            input_queues[cp] = q
            outconnections[cp] = {}
        
    # print 'PROCESS PES: %s' % process_pes
    mappedInputs = {}
    
    for node in graph.nodes():
        pe = node.getContainedObject()
        for edge in graph.edges(node, data=True):
            direction = edge[2]['DIRECTION']
            source = direction[0]
            source_output = edge[2]['FROM_CONNECTION']
            dest = direction[1]
            dest_processes=processes[dest]
            source_processes=processes[source]
                
            if dest == pe:
                # dest_input = edge[2]['TO_CONNECTION']
                numInputs[dest] += len(source_processes)
            if source == pe:
                allconnections = edge[2]['ALL_CONNECTIONS']
                for (source_output, dest_input) in allconnections:
                    communication = _getCommunication(source, dest, dest_input, process_pes, input_queues)
                    for cp in communication:
                        try:
                            outconnections[cp][source_output].append((dest_input, communication[cp]))
                        except KeyError:
                            outconnections[cp][source_output] = [(dest_input, communication[cp])]
                for cp in process_pes[pe].values():
                    cp.outputconnections[source_output]['writer']=QueueWriter(outconnections[cp][source_output])

        mappedInputs[pe] = inputs if pe in sources else None            
        
    # print 'INPUTQUEUES: %s' % input_queues
    # print 'OUTCONNECTIONS: %s' % outconnections
    # print 'MAPPED INPUTS: %s' % mappedInputs
    
    jobs = []
    for node in graph.nodes():
        pe = node.getContainedObject()
        for cp in process_pes[pe].values():
            p = multiprocessing.Process(target=_processWorker, args=(cp, mappedInputs[pe], input_queues[cp], numInputs[pe], outconnections[cp]))
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
    parser.add_argument('-d', '--data', metavar='inputdata', help='input dataset in JSON format')
    parser.add_argument('-i', '--iter', metavar='iterations', type=int, help='number of iterations')
    parser.add_argument('-s', '--simple', help='force simple processing', action='store_true')
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
    elif args.data:
        inputs = json.loads(args.data)
    elif args.iter:
        inputs = [ {} for i in range(args.iter) ]
        print "Processing %s iteration(s)" % args.iter
    else:
        print 'Processing 1 iteration.'
    
    print 'Starting multiprocessing with %s processes.' % args.num    
    print 'Inputs: %s' % inputs
    multiprocess(graph, args.num, inputs, args.simple)
