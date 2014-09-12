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
The dispel4py workflow graph.
'''

import networkx as nx
import inspect
import sys

from dispel4py.core import GenericPE 

class WorkflowNode:
    '''
    Wrapper class for workflow nodes - wraps around general python functions or, more specifically, subclasses
    of classes denoting PEs, e.g.: GenericPEs
    
    '''
    
    # Supported types of workflow nodes:
    WORKFLOW_NODE_PE = 0
    WORKFLOW_NODE_FN = 1
    WORKFLOW_NODE_CP = 2
    node_counter = 0
    
    def __init__(self, o):
        self.obj = o
        self.outputs = []
        self.inputs = []
        
        # TODO: This may not be accurate - check.
        if inspect.isroutine(o): # it's a 'function'
            self.nodeType = self.WORKFLOW_NODE_FN
        elif isinstance(o, GenericPE):
            # TODO Perhaps we should have a similar arrangement for annotating PEs and their ins/outs 
            o.id = o.name + str(WorkflowNode.node_counter)
            WorkflowNode.node_counter += 1
            self.nodeType = self.WORKFLOW_NODE_PE
            for i in o.inputconnections.values():
                self.inputs.append({})  # empty for the time being - only the index matters
            for i in o.outputconnections.values():
                self.outputs.append({})
        elif isinstance(o, WorkflowGraph):
            self.nodeType = self.WORKFLOW_NODE_CP
            try:
                for i in o.inputmappings:
                    self.inputs.append({})
                for i in o.outputmappings:
                    self.outputs.append({})
            except AttributeError:
                pass
        else:
            sys.stderr.write('Error: Unknown type of object passed as a Workflow Node: %s\n' % type(o))
            raise Exception("Unknown type of object passed as a Workflow Node: %s" % type(o))
            # sys.exit(1) # too harsh?
            
    def getContainedObject(self):
        ''' Returns the wrapped PE or function. '''
        return self.obj
        

# Used as attribute names
FROM_CONNECTION = 'from_connection'
TO_CONNECTION   = 'to_connection'
DIRECTION = 'direction'

class WorkflowGraph(object):
    """ A graph representing the workflow and related methods """

    def __init__(self):
        self.graph = nx.Graph()
        self.objToNode = {}
    
    def add(self, n):
        ''' 
        Adds node n, which should be a GenericPE of a python routine, and returns the created workflow node
        :rtype: WorkflowNode
        '''
        nd = WorkflowNode(n)
        self.graph.add_node(nd)
        self.objToNode[n] = nd
        return nd

    def connect(self, fromNode, fromConnection, toNode, toConnection):
        ''' 
        Connect the two given nodes from the given output to the given input. 
        If the nodes are not in the graph, they will be added.
        
        :param fromNode: the source PE of the connection
        :param fromConnection: the name of the output of the source node 'fromNode'
        :type fromConnection: String
        :param toNode: the destination PE of the connection
        :param toConnection: the name of the input of the destination node 'toNode'
        :type toConnection: String
        '''
        
        #  (TODO: If the nodes are already connected through alternative connections create a proxy node and connect them through it.)

        if not fromNode in self.objToNode:
            self.add(fromNode)
        if not toNode in self.objToNode:
            self.add(toNode)
        
        fromWfNode = self.objToNode[fromNode]
        toWfNode   = self.objToNode[toNode]
        
        if self.graph.has_edge(fromWfNode, toWfNode):
            self.graph[fromWfNode][toWfNode]['ALL_CONNECTIONS'].append((fromConnection, toConnection))
        else:
            self.graph.add_edge(fromWfNode, toWfNode, 
                **{'FROM_CONNECTION' : fromConnection, 'TO_CONNECTION' : toConnection, 'DIRECTION' : (fromNode,toNode), 
                'ALL_CONNECTIONS' : [(fromConnection, toConnection)]})
                    
    def getContainedObjects(self):
        nodes = [ node.getContainedObject() for node in self.graph.nodes() ]
        return sorted(nodes, key=lambda x: x.id)

    def propagate_types(self):
        '''
        Propagates the types throughout the graph by retrieving the output types from each node, starting
        from the root, and providing them to connected consumers.
        '''
        visited = set()
        for node in self.graph.nodes():
            if not node in visited:
                self.__assign_types(node, visited)

    def __assign_types(self, node, visited):
        pe = node.getContainedObject()
        inputTypes = {}
        for edge in self.graph[node].values():
            if pe == edge['DIRECTION'][1]:
                # pe is the destination so look up the types produced by the sources
                source = edge['DIRECTION'][0]
                sourceNode = self.objToNode[source]
                if not sourceNode in visited:
                    self.__assign_types(sourceNode, visited)
                inType = source.getOutputTypes()[edge['FROM_CONNECTION']]
                inputTypes[edge['TO_CONNECTION']]=inType
        pe.setInputTypes(inputTypes)
        visited.add(node)
        # print "%s: Assigned inputs = %s, received outputs = %s" % (pe.__class__.__name__, inputTypes, pe.getOutputTypes())  
        
    def flatten(self):
        '''
        Subgraphs contained within composite PEs are added to the top level workflow.
        '''
        hasComposites = True
        toRemove = set()
        while hasComposites:
            hasComposites = False
            toRemove = set()
            for node in self.graph.nodes():
                if node.nodeType == WorkflowNode.WORKFLOW_NODE_CP:
                    hasComposites = True
                    toRemove.add(node)
                    wfGraph = node.getContainedObject()
                    subgraph = wfGraph.graph
                    self.graph.add_nodes_from(subgraph.nodes(data=True))
                    self.graph.add_edges_from(subgraph.edges(data=True))
                    self.objToNode.update(wfGraph.objToNode)
                    for inputname in wfGraph.inputmappings:
                        toPE, toConnection = wfGraph.inputmappings[inputname]
                        edge = None
                        fromPE, fromConnection = None, None
                        for e in self.graph[node].values():
                            if wfGraph == e['DIRECTION'][1] and inputname == e['TO_CONNECTION']:
                                fromPE = e['DIRECTION'][0]
                                fromConnection = e['FROM_CONNECTION']
                                edge = self.objToNode[fromPE], self.objToNode[wfGraph]
                                break
                        if edge is not None:
                            # print 'connecting %s %s to %s %s' % (fromPE.boltId, fromConnection, toPE.boltId, toConnection)
                            self.connect(fromPE, fromConnection, toPE, toConnection)
                    for outputname in wfGraph.outputmappings:
                        fromPE, fromConnection = wfGraph.outputmappings[outputname]
                        destinations = []
                        for e in self.graph[node].values():
                            if wfGraph == e['DIRECTION'][0] and outputname == e['FROM_CONNECTION']:
                                toPE = e['DIRECTION'][1]
                                toConnection = e['TO_CONNECTION']
                                destinations.append((toPE, toConnection))
                        for (toPE, toConnection) in destinations:
                            # print 'connecting output %s.%s' % (toPE, toConnection)
                            self.connect(fromPE, fromConnection, toPE, toConnection)
            self.graph.remove_nodes_from(toRemove)
                
def _create_dot(graph, instanceNames={}, counter=0):
    dot = ''
    # assign unique names
    for node in graph.graph.nodes():
        try:
            name = node.getContainedObject().id, counter
        except:
            name = node.getContainedObject().__class__.__name__, counter
        instanceNames[node] = name
        counter += 1
    
    # now add all the nodes and their input and output connections
    cluster_index = 0
    for node in graph.graph.nodes():
        pe = node.getContainedObject()
        if isinstance(pe, WorkflowGraph):
            dot += _create_cluster(pe, cluster_index, instanceNames, counter)
            cluster_index += 1
            continue
            
        name, index = instanceNames[node]
        dot += name + str(index) + "[label=\"{ "
        # add inputs
        inputNames = []
        outputNames = []
        for edge in graph.graph[node].values():
            if pe == edge['DIRECTION'][1]:
                inputName = edge['TO_CONNECTION']
                dotName = "<in_" + inputName + ">" + inputName
                if dotName not in inputNames:
                    inputNames.append(dotName)
            else:
                outputName = edge['FROM_CONNECTION']
                dotName = "<out_" + outputName + ">" + outputName
                if dotName not in outputNames:
                    outputNames.append(dotName)
            
        if inputNames: dot += '{' + ' | '.join(inputNames) + '} | '
        dot += name
        if outputNames: dot += ' | {' + ' | '.join(outputNames) + '}'
        dot += " }\"];\n"
    
    # connect the inputs and outputs    
    for node in graph.graph.nodes():
        pe = node.getContainedObject()
        for edge in graph.graph[node].values():
            if pe == edge['DIRECTION'][0]:
                if isinstance(pe, WorkflowGraph):
                    inner_source, source_output = pe.outputmappings[edge['FROM_CONNECTION']]
                    node = pe.objToNode[inner_source]
                else:
                    source_output = edge['FROM_CONNECTION']
                # pe is the source so look up the connected destination
                dest = edge['DIRECTION'][1]
                if isinstance(dest, WorkflowGraph):
                    inner_dest, dest_input = dest.inputmappings[edge['TO_CONNECTION']]
                    destNode = dest.objToNode[inner_dest]
                else:
                    destNode = graph.objToNode[dest]
                    dest_input = edge['TO_CONNECTION']
                dot += '%s%s' % instanceNames[node] + ':out_' + source_output
                dot += ' -> '
                dot += '%s%s' % instanceNames[destNode] + ':in_' + dest_input +';\n'
    return dot
                        
def _create_cluster(graph, index, instanceNames, counter):
    dot = 'subgraph cluster_%s {\n' % index
    try:
        # names for composite PEs are optional
        dot += 'label = "%s";' % graph.name
    except:
        pass
    dot += 'style=filled;\n'
    dot += 'color=lightgrey;\n'
    if index % 2:
        dot += 'fillcolor=lightgrey;\n'
    dot += _create_dot(graph, instanceNames, counter)
    dot += '}\n'
    return dot
    
def draw(graph):
    '''
    Creates a representation of the workflow graph in the dot language.
    '''
    dot = 'digraph request\n{\nnode [shape=Mrecord, style=filled, fillcolor=white];\n'
    dot += _create_dot(graph)
    dot += '}\n'
    return dot

def drawDot(graph):
    '''
    Draws the workflow as a graph and creates a PNG image using graphviz dot.
    '''
    from subprocess import Popen, PIPE
    dot = draw(graph)
    p = Popen(['dot', '-T', 'png'], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate(dot.encode('utf-8'))
    return stdout
    
def getConnectedInputs(node, graph):
    names = []
    for edge in graph.edges(node, data=True):
        direction = edge[2]['DIRECTION']
        dest = direction[1] 
        dest_input = edge[2]['TO_CONNECTION']
        if dest == node.getContainedObject():
            names.append(dest_input)
    return names
