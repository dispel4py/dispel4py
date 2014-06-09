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

from verce.workflow_graph import WorkflowGraph
from verce.GenericPE import GenericPE, NAME, TYPE
from verce.simple_process import order, process, multiprocess
import re

class TestProducer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        out1 = { NAME: 'output1', TYPE: ['count']}
        self.outputconnections['output1'] = out1
        out2 = { NAME: 'output2', TYPE: ['count']}
        self.outputconnections['output2'] = out1
    def process(self, inputs):
        output = { 'output1' : [1], 'output2' : [2] }
        print "%s: Producing %s" % (self.name, output)
        return output

class TestConsumer(GenericPE):
    def setInputTypes(self, types):
        self.inputtypes = types
        for name in types:
            self.inputconnections[name] = { NAME : name }
    def getOutputTypes(self):
        ret = {}
        for i in range(1, len(self.inputtypes)+1):
            ret['output%s' % i] = self.inputtypes[ 'input%s' % i ]
        return ret
    def process(self, inputs):
        print '%s: input %s' % (self.name, inputs)
        output = {}
        for name, value in inputs.iteritems():
             output['output%s' % re.findall('\d+', name)[0]] = value
        print "%s: output %s" % (self.name, output)
        return output
        
if __name__ == '__main__':
    producer1 = TestProducer()
    producer1.name = 'Producer1'
    producer2 = TestProducer()
    producer2.name = 'Producer2'
    # producer.partition = "A"
    nodes = []
    for i in range(10):
        n = TestConsumer()
        n.name = 'Consumer%s' % i
        nodes.append(n)
        n.partition = "A"
    

    graph = WorkflowGraph()
    # graph.connect(words, "output", filter, "input")
    graph.connect(producer1, 'output1', nodes[0], 'input1')
    graph.connect(nodes[0], 'output1', nodes[1], 'input1')
    graph.connect(nodes[1], 'output1', nodes[2], 'input1')
    graph.connect(producer2, 'output2', nodes[3], 'input1')
    graph.connect(nodes[3], 'output1', nodes[4], 'input1')
    graph.connect(nodes[4], 'output1', nodes[5], 'input1')
    graph.connect(nodes[2], 'output1', nodes[6], 'input1')
    graph.connect(nodes[5], 'output1', nodes[6], 'input1')
    # graph.connect(nodes[2], 'output1', nodes[3], 'input1')
    
    ordered = order(graph)
    print 'Ordered graph:'
    print '-----------'
    for node in ordered:
        print node.obj.name
    print '-----------\n'
    
    # results = processConnected(graph, ordered)
    # print results
    results = process(graph)
    print results

    # results = process(graph, [{}, {}, {}])
    # print results
    # 
    # multiprocess(graph)
