# Copyright (c) The University of Edinburgh 2014
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#	 Unless required by applicable law or agreed to in writing, software
#	 distributed under the License is distributed on an "AS IS" BASIS,
#	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	 See the License for the specific language governing permissions and
#	 limitations under the License.

from __future__ import print_function
from verce.workflow_graph import WorkflowGraph
from verce.GenericPE import GenericPE, GROUPING

from mpi4py import MPI
import sys
import traceback

comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

def simpleLogger(msg):
    print(msg)

peProcess = {}

class SourceWrapper(object):
    def __init__(self, outputmappings, producer, numIterations=None):
        self.outputmappings = outputmappings
        self.producer = producer
        self.numIterations = numIterations
    def compute(self):
        x = 0
        while self.numIterations is None or x < self.numIterations:
            x+=1
            result = self.producer.process()
            if result:
                for output_name in result:
                    for (dest, input_name) in self.outputmappings[output_name]:
                        data = { input_name : result[output_name] }
                        print('%s: Sending data to %s' % (self.producer.name, dest))
                        comm.send(data, dest=dest)
                
        # send some kind of terminate message down the stream?
        print('%s: Completed processing.' % self.producer.name)
        sys.exit(0) 

class ProcessWrapper(object):
    def __init__(self, pe, master):
        self.pe = pe
        self.master = master
    def compute(self):
        while True:
            data=comm.recv(source=self.master)
            # print('Received %s from master %s' % (data, self.master))
            k=self.pe.process(data)
            comm.send(k, dest=self.master)
            # print('Returned results %s to master %s' % (k, self.master))
            
class MessageQueue(object):
    def __init__(self, inputmappings, outputmappings, workers):
        self.workers = workers
        self.availableWorkers = set(workers)
        self.dataQueue = []
        # input mapping is input_name -> grouping_func
        self.inputmappings = inputmappings
        # output mapping is output_name -> [ (to_rank, input_name) ]
        self.outputmappings = outputmappings
    def compute(self):
        while True:
            status = MPI.Status()
            result = comm.recv(status=status, source=MPI.ANY_SOURCE)
            msgSource = status.source
            # print('Received data %s from %s' % (result, msgSource))
            if msgSource in self.workers:
                # one of the workers has finished processing 
                # send the data on to the consumers
                # and add the worker to the available queue 
                if result:
                    for output_name in result:
                        try:
                            for (dest, input_name) in self.outputmappings[output_name]:
                                data = { input_name : result[output_name] }
                                print('Sending data %s to next PE %s' % (data, dest))
                                comm.isend(data, dest=dest)
                        except KeyError:
                            print('Discarding unconnected output %s' % result)
                            # ignoring this output as there's no PE connected to it
                            pass
                self.availableWorkers.add(msgSource)
            else:
                # we have an input data block
                self.dataQueue.append(result)

            # now apply the grouping function if there are workers available
            if self.availableWorkers:
                block = self.dataQueue.pop() if self.dataQueue else {}
                for input_name in self.inputmappings:
                    # grouping function depends on the input name
                    grouping_func = self.inputmappings[input_name]
                    data = { input_name : block[input_name] } if input_name in block else None
                    grouping_func.assign(data, self.availableWorkers)

class ShuffleDistribute(object):                    
    def assign(self, data, workers):
        if data and workers:
            sink = workers.pop()
            # print 'ShuffleDistribute: Sending data to worker %s' % sink
            comm.send(data, dest=sink)
            
class GroupByDistribute(object):
    def __init__(self, name, groupBy, workers):
        self.name = name
        self.groupBy = groupBy
        self.workers = tuple(workers)
        self.dataQueue = dict([x, []] for x in workers)
    def assign(self, data, availableWorkers):
        if data:
            group = [ data[self.name][x] for x in self.groupBy ]
            index = hash(tuple(group)) % len(self.workers)
            self.dataQueue[self.workers[index]].append(data)
        used = set()
        for sink in availableWorkers:
            try:
                # print('GroupByDistribute: Sending data to worker %s' % sink)
                if self.dataQueue[sink]:
                    inp = self.dataQueue[sink].pop()
                    # translate output to input
                    comm.send(inp, dest=sink)
            except ValueError:
                pass
        for w in used:
            availableWorkers.remove(w)
                    
def buildProcess(workflow, maxIterations=None):
    graph = workflow.graph
    
    node_count = graph.number_of_nodes()
    if size < node_count:
        # ok this is a bit crap, we need at least one process for each node in the graph
        print('Graph is too large for MPI node size: %s < %s' % (size, graph.size), file=sys.stderr)
        sys.exit(1)
        
    num_instances = size/node_count-1
    # print('Rank %s: building processes' % rank)
    # print ('Number of nodes: %s\nNumber of instances per PE: %s' % (node_count, num_instances))

    node_rank = {}
    workers = {}
    inputmappings = {}
    outputmappings = {}
        
    # give each node a unique rank
    node_counter = 0
    for node in graph.nodes():
        pe = node.getContainedObject()
        pe.log = simpleLogger
        node_rank[pe] = node_counter
        peProcess[node_counter] = pe
        if pe.inputconnections:
            workers[pe] = range(node_count+node_counter*num_instances, node_count+(node_counter+1)*num_instances)
        else:
            workers[pe] = []
        # print('Assigning workers %s to PE %s' % (workers[pe], pe.name))
        inputmappings[pe] = {}
        outputmappings[pe] = {}
        node_counter += 1
    
    # now connect PEs
    for edge in graph.edges(data=True):
        direction = edge[2]['DIRECTION']
        source = direction[0]
        source_output = edge[2]['FROM_CONNECTION']
        dest = direction[1]
        dest_input = edge[2]['TO_CONNECTION']
        # print("Assigning connection %s.%s to %s.%s" % (source.name, source_output, dest.name, dest_input))
        # an output can be broadcast to many PEs so there's a list of targets
        if source_output not in outputmappings[source]:
            outputmappings[source][source_output] = []
        outputmappings[source][source_output].append( (node_rank[dest], dest_input) )
            
        # find out the grouping of the input
        # shuffle grouping by default
        # this can be overridden by the PE implementation or when creating the topology
        grouping = ShuffleDistribute()
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                # fields grouping with the list of fields
                grouping = GroupByDistribute(dest_input, groupingtype, workers[dest])
            # elif groupingtype == 'all':
            #     grouping = AllDistribute
            # elif groupingtype == 'global':
            #     grouping = GlobalDistribute
            elif groupingtype == 'none':
                grouping = ShuffleDistribute()
        # print('Grouping for input %s.%s: %s' % (dest.name, dest_input, grouping))
        inputmappings[dest][dest_input] = grouping
        
    for node, data in graph.nodes(data=True):
        pe = node.getContainedObject()
        pe_rank = node_rank[pe]
        if not pe.inputconnections:
            # if there are no inputs it's a source :P
            # print('SourceWrapper for PE %s at rank %s' % (pe.name, pe_rank))
            peProcess[pe_rank] = SourceWrapper(outputmappings[pe], pe, maxIterations)
        else:
            # print('MessageQueue for PE %s at rank %s' % (pe.name, pe_rank))
            peProcess[pe_rank] = MessageQueue(inputmappings[pe], outputmappings[pe], workers[pe])
            for w in workers[pe]:
                # print('ProcessWrapper for PE %s at rank %s' % (pe.name, w))
                peProcess[w] = ProcessWrapper(pe, pe_rank)


from verce.utils import loadGraph          
from test.testing import WordCounter, RandomWordProducer 
               
if __name__ == "__main__":
    # the following should be done only once but how to distribute PE instances?
    graph = loadGraph(sys.argv[1], sys.argv[2])
    graph.flatten()
    
    # run 100 iterations
    buildProcess(graph, 100)
    # each worker now starts computing
    if rank in peProcess:
        print('My rank is %s and I am processing' % rank)
        peProcess[rank].compute()
    else:
        print('My rank is %s and I have nothing to do!' % rank)
        
    
    
    
