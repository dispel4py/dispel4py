import sys
import traceback
import types
from dispel4py.core import GROUPING

STATUS_ACTIVE = 10
STATUS_INACTIVE = 11
STATUS_TERMINATED = 12

def simpleLogger(self, msg):
    print("%s (rank %s): %s" % (self.id, self.rank, msg))

class GenericWrapper(object):

    def __init__(self, pe):
        self.pe = pe
        self.targets = {}

    def process(self):
        self.pe.preprocess()
        result = self._read()
        inputs, status = result
        while status != STATUS_TERMINATED:
            self.pe.log('Read result: %s, status=%s' % (inputs, status))
            if inputs is not None:
                outputs = self.pe.process(inputs)
                self.pe.log('Produced output: %s'% outputs)
                if outputs is not None:
                    for key, value in outputs.iteritems():
                        self._write(key, value)
            inputs, status = self._read()
        self.pe.postprocess()
        self._terminate()
        
    def _read(self):
        # check the provided inputs
        if self.provided_inputs is not None:
            if self.provided_inputs:
                return self.provided_inputs.pop(0), STATUS_ACTIVE
            else:
                return None, STATUS_TERMINATED
        
class ShuffleCommunication(object):
    def __init__(self, rank, sources, destinations):
        self.destinations=destinations
        self.currentIndex = (sources.index(rank) % len(self.destinations)) -1
    def getDestination(self, data):
        self.currentIndex = (self.currentIndex+1)%len(self.destinations)
        return [self.destinations[self.currentIndex]]

class GroupByCommunication(object):
    def __init__(self, destinations, input_name, groupby):
        self.groupby = groupby
        self.destinations=destinations
        self.input_name = input_name
    def getDestination(self,data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        dest_index=abs(make_hash(output))%len(self.destinations)
        return [self.destinations[dest_index]]

class AllToOneCommunication(object):
    def __init__(self, destinations):
        self.destinations=destinations
    def getDestination(self,data):
        return [self.destinations[0]]
        
class OneToAllCommunication(object):
    def __init__(self, destinations):
        self.destinations=destinations
    def getDestination(self,data):
        return self.destinations

def _getConnectedInputs(node, graph):
    names = []
    for edge in graph.edges(node, data=True):
        direction = edge[2]['DIRECTION']
        dest = direction[1] 
        dest_input = edge[2]['TO_CONNECTION']
        if dest == node.getContainedObject():
            names.append(dest_input)
    return names

def _getNumProcesses(size, numSources, numProcesses, totalProcesses):
    div = max(1, totalProcesses-numSources)
    return int(numProcesses * (size - numSources)/div)  
    
def _assign_processes(workflow, size):
    graph = workflow.graph
    processes={}
    success=True
    totalProcesses = 0
    numSources = 0
    sources = []
    for node in graph.nodes():
        pe=node.getContainedObject()
        # if pe.inputconnections:
        if _getConnectedInputs(node, graph):
            totalProcesses = totalProcesses + pe.numprocesses
        else:
            sources.append(pe.id)
            totalProcesses += 1
            numSources += 1
    
    if totalProcesses > size:
        success = False
        # we need at least one process for each node in the graph
        print 'Graph is too large for job size: %s > %s.' % (totalProcesses, size)
    else:    
        node_counter = 0
        for node in graph.nodes():
            pe=node.getContainedObject()
            prcs = 1 if pe.id in sources else _getNumProcesses(size, numSources, pe.numprocesses, totalProcesses)
            processes[pe.id]=range(node_counter, node_counter+prcs)
            node_counter = node_counter + prcs
    return success, sources, processes

def _getCommunication(rank, source_processes, dest, dest_input, dest_processes):
    communication = ShuffleCommunication(rank, source_processes, dest_processes)
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                communication = GroupByCommunication(dest_processes, dest_input, groupingtype)
            elif groupingtype == 'all':
                communication = OneToAllCommunication(dest_processes)
            elif groupingtype == 'global':
                communication = AllToOneCommunication(dest_processes)
    except KeyError:
        print("No input '%s' defined for PE '%s'" % (dest_input, dest.id))
        raise
    return communication
    
def _create_connections(graph, node, processes):
    pe=node.getContainedObject()
    inputmappings = { i : {} for i in processes[pe.id] }
    outputmappings = { i : {} for i in processes[pe.id] }
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
            for i in processes[pe.id]:
                for (source_output, dest_input) in allconnections:
                    try:
                        inputmappings[i][dest_input] += source_processes
                    except KeyError:
                        inputmappings[i][dest_input] = source_processes
        if source == pe:
            for i in processes[pe.id]:
                for (source_output, dest_input) in allconnections:
                    communication = _getCommunication(i, source_processes, dest, dest_input, dest_processes)
                    try:
                        outputmappings[i][source_output].append((dest_input, communication))
                    except KeyError:
                        outputmappings[i][source_output] = [(dest_input, communication)]
    return inputmappings, outputmappings

def _connect(workflow, processes):
    graph = workflow.graph
    outputmappings = {} 
    inputmappings = {}
    for node in graph.nodes():
        inc, outc = _create_connections(graph, node, processes)
        inputmappings.update(inc)
        outputmappings.update(outc)
    return inputmappings, outputmappings

class MultiProcessingWrapper(GenericWrapper):
    
    def __init__(self, rank, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.provided_inputs = provided_inputs

    def _read(self):
        result = super(MultiProcessingWrapper, self)._read()
        if result is not None:
            return result
        # read from input queue
        return self.input_queue.get()

    def _write(self, name, data):
        try:
            targets = self.targets[name]
        except KeyError:
            # no targets
            return
        for (inputName, communication) in targets:
            output = { inputName : data }
            dest = communication.getDestination(output)
            for i in dest:
                self.pe.log('Writing out %s' % output)
                self.output_queues[i].put((output, STATUS_ACTIVE))
                
    def _terminate(self):
        for output, targets in self.targets.iteritems():
            for (inputName, communication) in targets:
                for i in communication.destinations:
                    self.pe.log('Terminating consumer %s' % i)
                    self.output_queues[i].put((None, STATUS_TERMINATED))