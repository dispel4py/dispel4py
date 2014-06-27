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
Test graphs for execution with worker_mpi.
Run as follows::

    mpiexec -n <num_proc> python -m dispel4py.worker_mpi test.worker_mpi_test -a <graph>

where graph is the name of the graph to test, one of 'pipeline', 'square', 'tee' or 'twopipes'.
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.GenericPE import GenericPE, NAME

class TestProducer(GenericPE):
    def __init__(self, numOutputs=1):
        GenericPE.__init__(self)
        if numOutputs == 1:
            self.outputconnections = { 'output' : { NAME : 'output' } }
        else:
            for i in range(numOutputs):
                self.outputconnections['output%s' % i] = { NAME : 'output%s' % i } 
        self.counter = 0
    def process(self, inputs):
        self.counter += 1
        result = {}
        for output in self.outputconnections:
            result[output] = self.counter
        return result

class TestOneInOneOut(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
        self.outputconnections = { 'output' : { NAME : 'output' } }
    def process(self, inputs):
        # print '%s: Processing inputs %s' % (self, inputs)
        return { 'output' : inputs['input'] }

class TestOneInOneOutWriter(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
        self.outputconnections = { 'output' : { NAME : 'output' } }
    def process(self, inputs):
        self.write('output', inputs['input'])

class TestTwoInOneOut(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input0' : { NAME : 'input0' }, 'input1' : { NAME : 'input1' } }
        self.outputconnections = { 'output' : { NAME : 'output' } }
    def process(self, inputs):
        # print '%s: inputs %s' % (self.id, inputs)
        result = ''
        for inp in self.inputconnections:
            if inp in inputs:
                result += '%s' % (inputs[inp])
        if result:
            # print '%s: result %s' % (self.id, result)
            return { 'output' : result }

def testPipeline(graph):
    prod = TestProducer()
    prev = prod
    part1 = [prod]
    part2 = []
    for i in range(5):
        cons = TestOneInOneOut()
        part2.append(cons)
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    graph.partitions = [part1, part2]
    return graph
    
def testSquare():
    graph = WorkflowGraph()
    prod = TestProducer(2)
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOutWriter()
    last = TestTwoInOneOut()
    graph.connect(prod, 'output0', cons1, 'input')
    graph.connect(prod, 'output1', cons2, 'input')
    graph.connect(cons1, 'output', last, 'input0')
    graph.connect(cons2, 'output', last, 'input1')
    return graph
        
def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    prev = prod
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    return graph
    
def testUnconnected():
    graph = WorkflowGraph()
    testPipeline(graph)
    testPipeline(graph)
    del graph.partitions
    return graph
    
pipeline = testPipeline(WorkflowGraph())
square = testSquare()
tee = testTee()
twopipes = testUnconnected()
