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

# Tests for dispel4py.simple_process
'''
Using nose (https://nose.readthedocs.org/en/latest/) run as follows:
$ nosetests test/simple_process_test.py
...
----------------------------------------------------------------------
Ran 3 tests in 0.006s

OK
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.GenericPE import GenericPE, NAME
from dispel4py import simple_process

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
        return { 'output' : inputs['input'] }

class TestTwoInOneOut(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input0' : { NAME : 'input0' }, 'input1' : { NAME : 'input1' } }
        self.outputconnections = { 'output' : { NAME : 'output' } }
    def process(self, inputs):
        result = ''
        for inp in self.inputconnections:
            if inp in inputs:
                result += '%s' % (inputs[inp])
        if result:
            return { 'output' : result }

from nose import tools
def testPipeline():
    graph = WorkflowGraph()
    prod = TestProducer()
    prev = prod
    for i in range(5):
        cons = TestOneInOneOut()
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    results = simple_process.process(graph, [{}, {}, {}, {}, {}])
    counter = 1
    for output in results:
        tools.eq_({(prev.id, 'output'):[counter]}, output)
        counter += 1
    tools.eq_(6, counter)
    
def testSquare():
    graph = WorkflowGraph()
    prod = TestProducer(2)
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    last = TestTwoInOneOut()
    graph.connect(prod, 'output0', cons1, 'input')
    graph.connect(prod, 'output1', cons2, 'input')
    graph.connect(cons1, 'output', last, 'input0')
    graph.connect(cons2, 'output', last, 'input1')
    results = simple_process.process(graph, [{}, {}, {}, {}, {}])
    counter = 1
    for output in results:
        tools.eq_({(last.id, 'output'):[str(counter), str(counter)]}, output)
        counter += 1
        
def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    prev = prod
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    results = simple_process.process(graph, [{}, {}, {}, {}, {}])
    resultsIter = iter(results)
    for counter, output in zip(range(1, 6), results):
        print results
        output = resultsIter.next()
        tools.eq_({(cons1.id, 'output'):[counter], (cons2.id, 'output'):[counter]}, output)
    
