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

'''
Simple processor of Dispel4Py graphs. This processor determines the dependencies of each PE in the
graph and executes them sequentially.  

From the commandline, run the following command::

    python simple_process.py module [-h] [-a attribute] [-f inputfile] [-i iterations]
    
with parameters
 
:module: module that creates a Dispel4Py graph
:-a attr:   name of the graph attribute within the module (optional)
:-f file:   file containing input data in JSON format (optional)
:-i iter:   number of iterations to compute (default is 1)
:-h:      print this help page

For example::

    python -m verce.simple_process test.graph_testing.pipeline_test -i 5
    
    Processing 5 iteration(s)
    Starting simple processing.
    Inputs: [{}, {}, {}, {}, {}]
    Results: [{('TestOneInOneOut5', 'output'): [1]}, {('TestOneInOneOut5', 'output'): [2]}, {('TestOneInOneOut5', 'output'): [3]}, {('TestOneInOneOut5', 'output'): [4]}, {('TestOneInOneOut5', 'output'): [5]}]
    
'''

import networkx as nx
import types

storeResults = True

def _log(self, msg):
    print "%s: %s" % (self.id, msg)

class NullWriter(object): 
    def write(self, result):
        None

class SimpleWriter(object): 
    def __init__(self, outputName, pe, connections, data, results, teeToResult):
        self.outputName = outputName
        self.pe = pe
        self.connections = connections
        self.teeToResult = teeToResult
        self.data = data
        self.results = results
    def write(self, result):
        try:
            destinations = self.connections[(self.pe, self.outputName)]
            # handle a tee output which is a result
            if self.teeToResult:
                self._writeResult(result)
            for dest, inp_name in destinations:
                if dest in self.data:
                    self.data[dest].append({ inp_name : result })
                else:
                    self.data[dest] = [ { inp_name : result } ]
        except KeyError:
            self._writeResult(result)
    def _writeResult(self, result):
        # print ('%s: writing result = %s' % (self.pe.id, result))
        if storeResults:
            # output is not connected so we store it as an end result
            if (self.pe.id, self.outputName) in self.results:
                self.results[(self.pe.id, self.outputName)].append(result)
            else:
                self.results[(self.pe.id, self.outputName)] = [result]                    

def _getDependencies(graph, node, visited):
    pe = node.getContainedObject()
    for edge in graph.graph[node].values():
        if pe == edge['DIRECTION'][1]:
            # pe is the destination so look up the connected sources
            source = edge['DIRECTION'][0]
            sourceNode = graph.objToNode[source]
            if sourceNode not in visited:
                _getDependencies(graph, sourceNode, visited)
                visited.append(sourceNode)
       
def _getTargets(graph, node):
    pe = node.getContainedObject()
    result = []
    for edge in graph.graph[node].values():
        if pe == edge['DIRECTION'][0]:
            dest = edge['DIRECTION'][1]
            result.append(graph.objToNode[dest])
    return result
             
def order(graph, subgraph=None):
    ''' 
    Returns a list of the PEs in the given subgraph, ordered by dependencies.
    If no subgraph is provided the entire graph is ordered.
    '''
    if subgraph is None: subgraph = graph.graph.nodes()
    ordered = []
    for node in subgraph:
        pe = node.getContainedObject()
        if not _getTargets(graph, node):
            # we have a sink in the graph
            dep = []
            _getDependencies(graph, node, dep)
            for n in ordered: 
                try:
                    dep.remove(n)
                except:
                    # never mind if the element wasn't in the list
                    pass
            ordered += dep
            ordered.append(node)
    return ordered
            
def _initProcessingElements(graph, nodes):
    connections = {}
    roots = set(nodes)
    for node in nodes:
        pe = node.getContainedObject()
        pe.log = types.MethodType(_log, pe)
        for edge in graph.graph[node].values():
            if pe == edge['DIRECTION'][1]:
                # pe is the destination so look up the input name
                input_name = edge['TO_CONNECTION']
                # there might be more than one destination for this output (Tee)
                # so we create a list of destinations
                try:
                    connections[(edge['DIRECTION'][0], edge['FROM_CONNECTION'])].append((node, input_name))
                except:
                    connections[(edge['DIRECTION'][0], edge['FROM_CONNECTION'])] = [(node, input_name)]
                # pe has inputs so we remove it from roots list
                try:
                    roots.remove(node)
                except:
                    pass
        pe.preprocess()
    return connections, roots
    
def _processInput(nodes, connections, resultconnections, inputData={}):
    data = inputData
    results = {}
    for node in nodes:
        pe = node.getContainedObject()
        # print 'Processing %s and input %s' % (pe, inputData)
        if node in data:
            inp = data[node]
        else:
            # no input for the node so we don't process
            continue
        for output_name in pe.outputconnections:
            teeToResult = (pe.id, output_name) in resultconnections
            pe.outputconnections[output_name]['writer']=SimpleWriter(output_name, pe, connections, data, results, teeToResult)
        while inp:
            block = inp.pop(0)
            try:
                # print 'input = %s' % block 
                outp = pe.process(block)
                # print 'output = %s' % outp
            except:
                # print 'Processing of %s failed : inputs = %s' % (pe.name, block)
                raise
        
            # translate from input to output
            if outp is not None:
                for name, output_data in outp.iteritems():
                    pe.write(name, output_data)
        del data[node]
    return results

def _processConnected(graph, nodes, inputs, provideAllInputs, resultconnections):
    # print 'Process connected : %s' % inputs
    connections, roots = _initProcessingElements(graph, nodes)
    results = []
    for inp in inputs:
        if not provideAllInputs:
            for node in roots:
                # if the node is root of the graph and doesn't have inputs
                # we pass an empty input to make it process once
                if not node in inp:
                    inp[node] = [{}]
        # print 'Processing connected : input = %s' % inp
        output = _processInput(nodes, connections, resultconnections, inp)
        results.append(output)
    return results

def process(graph, inputs=[{}], provideAllInputs=False, resultconnections=[]):
    '''
    Processes the given inputs in the graph.
    Any data produced by unconnected outputs are returned.
    
    :param graph: graph to process
    :param inputs: input data to the graph
    :param provideAllInputs: indicates whether *all* input data is provided explicitly for the graph 
           (meaning that an error is raised if a PE is expecting input but none is provided) or not 
           (i.e. an empty input block is provided for PEs expecting inputs)
    :param resultconnections: results of already processed iterations
    '''
    components = nx.connected_components(graph.graph)
    results = [ {} for i in inputs ]
    for comp in components:
        ordered = order(graph, comp)
        # print "Processing component: %s" % [ node.obj.name for node in ordered ]
        compResults = _processConnected(graph, ordered, inputs, provideAllInputs, resultconnections)
        for cr, r in zip(compResults, results):
            r.update(cr)
    return results

import multiprocessing

def processWorker(graph, nodes, inputs, result_queue):
    try:
        ordered = order(graph, nodes)
        results = processConnected(graph, ordered, inputs)
    except:
        results = {}
    # print results
    result_queue.put(results)

def multiprocess(graph, inputs=[{}], partitioned=False):
    '''
    Executes the given inputs in the the graph in multiple processes.
    If the graph is partitioned, i.e. every node has been assigned to a partition by giving a value
    to pe.partition, each partition is executed in a separate process. 
    If the graph is not partitioned each connected component is executed in a separate process.
    Results of each partition are communicated to the main process and returned by the method.
    '''
    if partitioned:
        partitions = {}
        for node in graph.graph.nodes():
            p = node.getContainedObject().partition
            if p in partitions:
                partitions[p].append(node)
            else:
                partitions[p] = [node]
        components = partitions.values()
    else:
        # partition into connected components
        components = nx.connected_components(graph.graph)

    jobs = []
    result_queue = multiprocessing.Queue()
    for nodes in components:
        p = multiprocessing.Process(target=processWorker, args=(graph, nodes, inputs, result_queue))
        jobs.append(p)
        # print p

    for j in jobs:
        j.start()
        
    results=[result_queue.get() for c in components]
    for j in jobs:
        j.join()
        
    return results
    
from verce.GenericPE import GenericPE
class SimpleLockstepWrapper(GenericPE):
    ''' 
    A PE that provides the input data in lockstep to the 'wrapped' PE.
    '''
    def __init__(self, name, wrapped):
        GenericPE.__init__(name)
        self.wrappedPE = wrapped
        self.inputconnections = wrapped.inputconnections
        self.outputconnections = wrapped.outputconnections
        self.cachedInputs = {}
        for name in self.inputconnections:
            self.cachedInputs[name] = []
    
    def preprocess(self):
        self.wrappedPE.outputconnections[name][WRITER] = self.outputconnections[name][WRITER]
        self.wrappedPE.preprocess()
    
    def process(inputs):
        for key, value in inputs.iteritems():
            self.cachedInputs[key].append(value)
        haveTuple = True
        for key in self.cachedInputs:
            if not self.cachedInputs[key]:
                haveTuple = False
        if haveTuple:
            lockstepInputs = {}
            for key, value in self.cachedInputs.iteritems():
                lockstepInputs[key] = value.pop(0)
                
            return self.wrappedPE.process(lockstepInputs)

if __name__ == "__main__":
    import argparse
    from verce.utils import loadGraph
    
    parser = argparse.ArgumentParser(description='Simple processing of a dispel4py graph in sequence.')
    parser.add_argument('module', help='module that creates a dispel4py graph')
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
            if rank == 0: print("Processing input file %s" % args.file)
        except:
            print('Cannot read input file %s' % args.file)
            sys.exit(1)
    elif args.iter:
        inputs = [ {} for i in range(args.iter) ]
        print "Processing %s iteration(s)" % args.iter
    else:
        print 'Processing 1 iteration.'
    
    print 'Starting simple processing.'    
    print 'Inputs: %s' % inputs
    results = process(graph, inputs)
    print 'Results: %s' % results
