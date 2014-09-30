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

# Run this with 
# mpiexec -n 4 python -m dispel4py.worker_mpi test.graph_testing.pipeline_test [-i <number of iterations>]

'''
Enactment of dispel4py graphs with MPI.
'''

from __future__ import print_function
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.core import GenericPE, NAME, GROUPING
from dispel4py.utils import make_hash

from mpi4py import MPI

import argparse
import sys
import traceback
import types
import json
import copy
from itertools import chain

comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()
name = MPI.Get_processor_name()

def simpleLogger(self, msg):
    print("%s (rank %s): %s" % (self.id, rank, msg))
    
TERMINATE_MSG='dispel4py.__terminate_process__'

def getConnectedInputs(node, graph):
    names = []
    for edge in graph.edges(node, data=True):
        direction = edge[2]['DIRECTION']
        dest = direction[1] 
        dest_input = edge[2]['TO_CONNECTION']
        if dest == node.getContainedObject():
            names.append(dest_input)
    return names

class ProcessingWrapper(object):
    def __init__(self, pe, numInputs, outputmappings, inputs=None):
        self.pe = pe
        self.nonTerminated = numInputs
        self.outputmappings = outputmappings
        # if pe.inputconnections:
        if inputs is None:
            print("%s (rank %s): I'm a bolt" % (pe.id, rank))
            self.receiveWrapper = receiveWrapper
            # self.maxIterations = None
        else:
            print("%s (rank %s): I'm a spout" % (pe.id, rank))   
            self.receiveWrapper = lambda : inputs.pop(0) if inputs else None
            # self.maxIterations = maxIterations
    def compute(self):
        self.pe.preprocess()
        i = 0
        hasFinished = False
        while True:
        # while self.maxIterations is None or i < self.maxIterations:
            input=self.receiveWrapper()
            # print("%s (rank %s): Processing input %s" % (self.pe.id, rank, input))
            if input == TERMINATE_MSG:
                self.nonTerminated -= 1
                if self.nonTerminated <= 0:
                    self.pe.postprocess()
                    self.terminateChildren()
                    hasFinished = True
                    break
                continue
            result = processWrapper(self.pe,input)
            if result is not None:
                for output_name in result:
                    if output_name in self.outputmappings:
                        # there might be more than one target in case there's an implicit Tee
                        for (nextProcesses, inputName, communication) in self.outputmappings[output_name]:
                            output = { inputName : result[output_name] }
                            communication.getDestination(output)
                            distributionWrapper(output,process.rank_dest)
            i += 1
        print("%s (rank %s): Processed %s input block(s)" % (self.pe.id, rank, i))
        if not hasFinished:
            self.terminateChildren()
    def terminateChildren(self):
        # send terminate messages downstream
        for output_name in self.outputmappings:
            for (nextProcesses, inputName, communication) in self.outputmappings[output_name]:
            #nextProcesses, inputName, communication = self.outputmappings[output_name]
                print ('Rank %s: Sending terminate message to %s' % (rank, nextProcesses))
                distributionWrapper(TERMINATE_MSG, nextProcesses)
    

class MPIWriter(object):
    def __init__(self, targets):
        self.targets = targets
    def write(self, result):
        if result:
            for (nextProcesses, inputName, communication) in self.targets:
                output = { inputName : result }
                # print('rank %s: Writing %s to %s' % (rank, output, nextProcesses))
                communication.getDestination(output)
                distributionWrapper(output,process.rank_dest)

class NullWriter(object): 
    def write(self, result):
        None

class ShuffleCommunication(object):
    def __init__(self, process, source_processes, processes_list):
        self.process=process
        self.processes_list=processes_list
        self.currentIndex = (source_processes.index(rank) % len(self.processes_list)) -1
    def getDestination(self,data):
        self.currentIndex = (self.currentIndex+1)%len(self.processes_list)
        self.process.rank_dest = [self.processes_list[self.currentIndex]]

class GroupByCommunication(object):
    def __init__(self, process, processes_list, input_name, groupby):
        self.groupby = groupby
        self.process=process
        self.processes_list=processes_list
        self.input_name = input_name
    def getDestination(self,data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        next=abs(make_hash(output))%len(self.processes_list)
        self.process.rank_dest=[self.processes_list[next]]

class AllToOneCommunication(object):
    def __init__(self, process, processes_list):
        self.process=process
        self.processes_list=processes_list

    def getDestination(self,data):
        self.process.rank_dest=[self.processes_list[0]]
        
class OneToAllCommunication(object):
    def __init__(self, process, processes_list):
        self.process=process
        self.processes_list=processes_list
    def getDestination(self,data):
        self.process.rank_dest=self.processes_list

class ProcessToNode():
    def __init__(self,rank):
        self.rank=rank
        self.rank_dest=None
        self.rank_source=None


def getNumProcesses(numSources, numProcesses, totalProcesses):
    div = max(1, totalProcesses-numSources)
    return int(numProcesses * (size - numSources)/div)  
    
def assign(workflow):
    graph = workflow.graph
    processes={}
    success=True
    totalProcesses = 0
    numSources = 0
    sources = []
    for node in graph.nodes():
        pe=node.getContainedObject()
        # if pe.inputconnections:
        if getConnectedInputs(node, graph):
            totalProcesses = totalProcesses + pe.numprocesses
        else:
            sources.append(pe.id)
            totalProcesses += 1
            numSources += 1
    
    if totalProcesses > size:
        success = False
        # we need at least one process for each node in the graph
        print('Graph is too large for MPI job size: %s > %s.' % (totalProcesses, size))
        # sys.exit(1)
    else:    
        node_counter = 0
        for node in graph.nodes():
            pe=node.getContainedObject()
            prcs = 1 if pe.id in sources else getNumProcesses(numSources, pe.numprocesses, totalProcesses)
            processes[pe.id]=range(node_counter, node_counter+prcs)
            node_counter = node_counter + prcs
    return success, sources, processes
    
def getCommunication(process, source_processes, dest, dest_input, dest_processes):
    communication = ShuffleCommunication(process, source_processes, dest_processes)
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                communication = GroupByCommunication(process, dest_processes, dest_input, groupingtype)
            elif groupingtype == 'all':
                communication = OneToAllCommunication(process, dest_processes)
            elif groupingtype == 'global':
                communication = AllToOneCommunication(process, dest_processes)
    except KeyError:
        print("No input '%s' defined for PE '%s'" % (dest_input, dest.id))
        raise
    return communication
   
def buildProcess(workflow, processes, sourceInputs):
    groups = {}
    for peid, procs in processes.iteritems():
        group = comm.Get_group()
        newgroup = group.Incl(procs)
        if rank == 0: print('creating communicator group %s' % procs)
        groups[peid] = comm.Create(newgroup)
    graph = workflow.graph
    for node in graph.nodes():
        pe=node.getContainedObject()
        for output_name in pe.outputconnections:
            pe.outputconnections[output_name]['writer']=NullWriter()
        myprocesses=processes[pe.id]
        if rank in myprocesses:
            pe.log = types.MethodType(simpleLogger, pe)
            pe.log('Starting to process')
            outputmappings = {} 
            numInputs = 0
            for edge in graph.edges(node, data=True):
                direction = edge[2]['DIRECTION']
                source = direction[0]
                source_output = edge[2]['FROM_CONNECTION']
                dest = direction[1]
                dest_processes=processes[dest.id]
                source_processes=processes[source.id]
                dest_input = edge[2]['TO_CONNECTION']
                allconnections = edge[2]['ALL_CONNECTIONS']
                if dest == pe:
                    numInputs += len(source_processes) * len(allconnections)
                if source == pe:
                    for (source_output, dest_input) in allconnections:
                        communication = getCommunication(process, source_processes, dest, dest_input, dest_processes)
                        try:
                            outputmappings[source_output].append((dest_processes, dest_input, communication))
                        except KeyError:
                            outputmappings[source_output] = [(dest_processes, dest_input, communication)]
                        pe.outputconnections[source_output]['writer']=MPIWriter(outputmappings[source_output])
            inputs = sourceInputs if numInputs == 0 else None
            pe.comm = groups[pe.id]
            wrapper=ProcessingWrapper(pe, numInputs, outputmappings, inputs)
            wrapper.compute()
            print ('%s (rank %s): Completed.' % (pe.id, rank))
            
          
##############################################################
# Simple processing

from dispel4py import simple_process
simple_process._log = simpleLogger
          
class GraphWrapperPE(GenericPE):

    def getcomm(self): return self.__comm
    def setcomm(self, value): 
        self.__comm = value
        for node in self.workflow.graph.nodes():
            node.getContainedObject().comm = value
    def delcomm(self): del self.__comm
    comm = property(getcomm, setcomm, delcomm)

    def __init__(self, workflow, inputmappings={}, outputmappings={}):
        GenericPE.__init__(self)
        self.workflow = workflow
        for input_name in inputmappings:
            self.inputconnections[input_name] = { NAME : input_name }
        for output_name in outputmappings.values():
            self.outputconnections[output_name] = { NAME : output_name }
        for node in workflow.graph.nodes():
            pe = node.getContainedObject()
            pe.log = types.MethodType(simpleLogger, pe)
        self.inputmappings = inputmappings
        self.outputmappings = outputmappings
        
    def preprocess(self):
        simple_process.preprocessComposite(self.workflow)

    def postprocess(self):
        simple_process.postprocessComposite(self.workflow)

    def process(self, inputs):
        # print ('%s (rank %s): processing inputs %s' % (self.id, rank, inputs))
        mappedInputs = {}
        for input_name in inputs:
            try:
                for pe, name in self.inputmappings[input_name]:
                    node = self.workflow.objToNode[pe]
                    try:
                        mappedInputs[node].append( { name : inputs[input_name] } )
                    except KeyError:
                        mappedInputs[node] = [ { name : inputs[input_name] } ]
            except KeyError:
                # if there's no mapping for an input we ignore it
                pass
        resultconnections = [ (resultPE, resultName) for (resultPE, resultName) in self.outputmappings ]
        # print ('%s (rank %s): result connections %s' % (self.id, rank, resultconnections))
        for node in self.workflow.graph.nodes():
            if not node.getContainedObject().inputconnections: mappedInputs[node] = [{}]
        # print ('%s (rank %s): mapped inputs %s' % (self.id, rank, mappedInputs))
        # results = simple_process.process(self.workflow, [ mappedInputs ], True, resultconnections)
        results = simple_process.processComposite(self.workflow, [ mappedInputs ], resultconnections)
        # print ('%s (rank %s): results %s' % (self.id, rank, results))
        
        for result in results:
            for output_pe, output_name in result:
                try:
                    wrapper_output = self.outputmappings[(output_pe, output_name)]
                    for block in result[(output_pe, output_name)]:
                        # print('Writing to %s output data %s) }' % (wrapper_output, block))
                        self.write(wrapper_output, block)
                except KeyError:
                    # if there's no output mapping then the output is not connected
                    # so we ignore it
                    # print('Discarding output (%s, %s)' % (output_pe, output_name))
                    pass
      
def simpleProcess(graph, sources, inputs):
    '''
    This method is used if there are less MPI processes than the nodes in the graph (PE instances).
    '''
    uberWorkflow = WorkflowGraph()
    wrappers = {}
    externalConnections = []
    partitions = []
    try:
        partitions = graph.partitions
    except AttributeError: 
        sourcePartition = []
        otherPartition = []
        for node in graph.graph.nodes():
            pe = node.getContainedObject()
            if pe.id in sources:
                sourcePartition.append(pe)
            else:
                otherPartition.append(pe)
        partitions = [sourcePartition, otherPartition]
    if rank == 0:
        print('Partitions: ', ', '.join(('[%s]' % ', '.join((pe.id for pe in part)) for part in partitions)))

    mappedInput = copy.deepcopy(inputs)
    for component in partitions:
        inputnames = {}
        outputnames = {}
        workflow = copy.deepcopy(graph)
        componentIds = []
        
        for pe in component:
            componentIds.append(pe.id)
            
        # print('component: %s' % componentIds)
        # print('inputs: %s' % inputs)
        
        for node in workflow.graph.nodes():
            pe = node.getContainedObject()
            if pe.id in componentIds:
                for edge in workflow.graph.edges(node, data=True):
                    direction = edge[2]['DIRECTION']
                    source = direction[0]
                    source_output = edge[2]['FROM_CONNECTION']
                    dest = direction[1]
                    dest_input = edge[2]['TO_CONNECTION']
                    if dest == pe and source.id not in componentIds:
                        try:
                            inputnames[dest.id + '_' + dest_input].append((dest, dest_input))
                        except KeyError:
                            inputnames[dest.id + '_' + dest_input] = [(dest, dest_input)]
                    elif source == pe and dest.id not in componentIds:
                        outputnames[(source.id, source_output)] = source.id + '_' + source_output
                        try:
                            grouping = dest.inputconnections[dest_input][GROUPING]
                        except KeyError:
                            grouping = None
                        externalConnections.append((source.id, source_output, dest.id, dest_input, grouping))
                   
                if pe.id in sources and mappedInput is not None:
                    for name in pe.inputconnections:
                        inputnames[pe.id + '_' + name] = [(pe, name)]
                    for block, mappedblock in zip(inputs, mappedInput):
                        if block == TERMINATE_MSG:
                            if mappedblock != TERMINATE_MSG:
                                del mappedInput[-1]
                                mappedInput.append(TERMINATE_MSG)
                            continue
                        for input_name in block:
                            mappedblock[pe.id + '_' + input_name] = block[input_name]
                    # print('Mapped input: %s' % mappedInput)

        for node in workflow.graph.nodes():
            if node.getContainedObject().id not in componentIds:
                workflow.graph.remove_node(node)
        # print ("inputnames : %s" % inputnames)
        wrapperPE = GraphWrapperPE(workflow, inputnames, outputnames)
        for node in workflow.graph.nodes():
            wrappers[node.getContainedObject().id] = wrapperPE
    
    # print ('External connections: %s' % externalConnections)
    for (source_id, source_output, dest_id, dest_input, grouping) in externalConnections:
        sourceWrapper = wrappers[source_id]
        destWrapper = wrappers[dest_id]
        if grouping:
            destWrapper.inputconnections[dest_id + '_' + dest_input][GROUPING] = grouping
        uberWorkflow.connect(sourceWrapper, source_id + '_' + source_output, destWrapper, dest_id + '_' + dest_input)
        # print ('%s: connected %s to %s' % (rank, sourceWrapper.id + '.' + source_id + '_' + source_output,
        #          destWrapper.id + '.' + dest_id + '_' + dest_input))
        
    if rank == 0:
        for node in uberWorkflow.graph.nodes():
            wrapperPE = node.getContainedObject()
            print('%s contains %s' % (wrapperPE.id, [n.getContainedObject().id for n in wrapperPE.workflow.graph.nodes()]))

    success = True
    processes = {}
    if rank == 0:
        success, sources, processes = assign(uberWorkflow)
    success=comm.bcast(success, root=0)
    if success: 
        if rank == 0: print ('Processes:', processes)
        processes=comm.bcast(processes,root=0)
        buildProcess(uberWorkflow, processes, mappedInput)
    else:
        print('Simple processing: Not enough MPI processes.')

def processWrapper(pe,input=None):
    output=pe.process(input)
    # print ('Rank %s: my processed output is %s' % (rank, output))
    return output


def receiveWrapper():
    input=comm.recv(source=MPI.ANY_SOURCE)
    # print ('Rank %s: Received %s' % (rank, str(input)[:100]))
    return input

# from dispel4py.utils import total_size
def distributionWrapper(data,rank_list):
    # print("Rank %s: sending %s to %s" % (rank, str(data)[:100], rank_list))
    for i in rank_list:
        request=comm.isend(data, dest=i)
        status = MPI.Status()
        request.Wait(status)

##############################################################
from dispel4py.utils import loadGraph          
               
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Submit a dispel4py graph for processing with MPI.')
    parser.add_argument('module', help='module that creates a dispel4py graph')
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
            if rank == 0: print("Processing input file %s" % args.file)
        except:
            print('Cannot read input file %s' % args.file)
            sys.exit(1)
    elif args.data:
        inputs = json.loads(args.data)
    elif args.iter:
        inputs = [ {} for i in range(args.iter) ]
        if rank == 0: print("Processing %s iterations" % args.iter)
        
    if type(inputs) == list:
        inputs += [TERMINATE_MSG]
    else:
        inputs = [inputs, TERMINATE_MSG]
    process=ProcessToNode(rank)
    
    # give each node a unique rank
    processes={}
    success=True
    sources = []
    if rank == 0:
        success, sources, processes = assign(graph)
    success=comm.bcast(success, root=0)
    if success and not args.simple:
        if rank == 0: print ('Processes:', processes)
        processes=comm.bcast(processes,root=0)
        buildProcess(graph, processes, inputs)
    else:
        if rank == 0: print('Start simple processing.')
        sources=comm.bcast(sources, root=0)
        simpleProcess(graph, sources, inputs)
            
