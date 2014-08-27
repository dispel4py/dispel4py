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
Sequential processing wrapper for graph partitions.
'''


##############################################################
# Simple processing wrapper for graph partitions
##############################################################

import copy
import types

from dispel4py.core import GenericPE, NAME, GROUPING
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process

def _log(self, msg):
    print("%s: %s" % (self.id, msg))

# Assign a different logger if required
simple_process._log = _log
          
class GraphWrapperPE(GenericPE):

    def __init__(self, workflow, inputmappings={}, outputmappings={}):
        GenericPE.__init__(self)
        self.workflow = workflow
        for input_name in inputmappings:
            self.inputconnections[input_name] = { NAME : input_name }
        for output_name in outputmappings.values():
            self.outputconnections[output_name] = { NAME : output_name }
        for node in workflow.graph.nodes():
            pe = node.getContainedObject()
            pe.log = types.MethodType(_log, pe)
        self.inputmappings = inputmappings
        self.outputmappings = outputmappings
        
    def preprocess(self):
        simple_process.preprocessComposite(self.workflow)

    def postprocess(self):
        simple_process.postprocessComposite(self.workflow)

    def process(self, inputs):
        # self.log('processing inputs %s' % inputs)
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
        # self.log('result connections %s' % resultconnections)
        for node in self.workflow.graph.nodes():
            if not node.getContainedObject().inputconnections: mappedInputs[node] = [{}]
        # self.log('mapped inputs %s' % mappedInputs)
        results = simple_process.processComposite(self.workflow, [ mappedInputs ], resultconnections)
        # self.log('results %s' % results)
        
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
            if pe in sources:
                sourcePartition.append(pe)
            else:
                otherPartition.append(pe)
        partitions = [sourcePartition, otherPartition]
    print 'Partitions: ', ', '.join(('[%s]' % ', '.join((pe.id for pe in part)) for part in partitions))
    sources = [ pe.id for pe in sources ]

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
        
        # print 'MAPPED INPUT BEFORE: %s' % mappedInput
        # print 'SOURCES : %s' % sources
        
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
                        for input_name in block:
                            mappedblock[pe.id + '_' + input_name] = block[input_name]

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
        
    return uberWorkflow, mappedInput
