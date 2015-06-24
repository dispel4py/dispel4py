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
Tests for simple sequential processing engine.

Using nose (https://nose.readthedocs.org/en/latest/) run as follows::

    $ nosetests dispel4py/test/simple_process_test.py
    ....
    ----------------------------------------------------------------------
    Ran 4 tests in 0.003s

    OK
'''

from dispel4py.examples.graph_testing.testing_PEs\
    import TestProducer, TestOneInOneOut, TestOneInOneOutWriter, \
    TestTwoInOneOut, TestIterative, IntegerProducer, PrintDataConsumer, \
    RandomWordProducer, RandomFilter, WordCounter

from dispel4py.new import simple_process
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import create_iterative_chain, CompositePE

from nose import tools


def testPipeline():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    results = simple_process.process_and_return(graph, inputs={prod: 5})
    tools.eq_({cons2.id: {'output': list(range(1, 6))}}, results)


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
    results = simple_process.process_and_return(graph, {prod: 1})
    tools.eq_({last.id: {'output': ['1', '1']}}, results)


def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    results = simple_process.process_and_return(graph, {prod: 5})
    tools.eq_(
        {cons1.id: {'output': list(range(1, 6))},
         cons2.id: {'output': list(range(1, 6))}},
        results)


def testWriter():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons1 = TestOneInOneOutWriter()
    graph.connect(prod, 'output', cons1, 'input')
    results = simple_process.process_and_return(graph, {prod: 5})
    tools.eq_({cons1.id: {'output': list(range(1, 6))}}, results)


def testIterative():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestIterative()
    graph.connect(prod, 'output', cons, 'input')
    results = simple_process.process_and_return(graph, {prod: 25})
    tools.eq_({cons.id: {'output': list(range(1, 26))}}, results)


def testProducer():
    graph = WorkflowGraph()
    prod = IntegerProducer(5, 234)
    cons = TestIterative()
    graph.connect(prod, 'output', cons, 'input')
    results = simple_process.process_and_return(graph, {prod: 1})
    tools.eq_({cons.id: {'output': list(range(5, 234))}}, results)


def testConsumer():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = PrintDataConsumer()
    graph.connect(prod, 'output', cons, 'input')
    results = simple_process.process_and_return(graph, {prod: 10})
    tools.eq_({}, results)


def testInputsAndOutputs():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestOneInOneOut()
    cons._add_output('output', tuple_type=['integer'])
    cons._add_input('input', grouping=[0], tuple_type=['integer'])
    cons.setInputTypes({'input': ['number']})
    tools.eq_({'output': ['number']}, cons.getOutputTypes())
    cons._add_output('output2')
    try:
        cons.getOutputTypes()
    except Exception:
        pass
    graph.connect(prod, 'output', cons, 'input')
    results = simple_process.process_and_return(graph, {prod: 10})
    tools.eq_({cons.id: {'output': list(range(1, 11))}}, results)


def testCreateChain():

    def add(a, b):
        return a + b

    def mult(a, b):
        return a * b

    def is_odd(a):
        return a % 2 == 1

    c = [(add, {'b': 1}), (mult, {'b': 3}), is_odd]
    chain = create_iterative_chain(c)
    prod = TestProducer()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', chain, 'input')
    graph.flatten()
    results = simple_process.process_and_return(graph, {prod: 2})
    for key, value in results.items():
        tools.eq_({'output': [False, True]}, value)


def testComposite():
    comp = CompositePE()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    comp.connect(cons1, 'output', cons2, 'input')
    comp._map_input('comp_input', cons1, 'input')
    comp._map_output('comp_output', cons2, 'output')
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', comp, 'comp_input')
    graph.connect(comp, 'comp_output', cons, 'input')
    graph.flatten()
    results = simple_process.process_and_return(graph, {prod: 10})
    tools.eq_({cons.id: {'output': list(range(1, 11))}}, results)


def testCompositeWithCreate():

    def create_graph(graph):
        cons1 = TestOneInOneOut()
        cons2 = TestOneInOneOut()
        graph.connect(cons1, 'output', cons2, 'input')
        graph._map_input('comp_input', cons1, 'input')
        graph._map_output('comp_output', cons2, 'output')

    comp = CompositePE(create_graph)
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', comp, 'comp_input')
    graph.connect(comp, 'comp_output', cons, 'input')
    graph.flatten()
    results = simple_process.process_and_return(graph, {prod: 10})
    tools.eq_({cons.id: {'output': list(range(1, 11))}}, results)


def testCompositeWithCreateParams():
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()

    def create_graph(graph, connections):
        for i in range(connections):
            graph.connect(cons1, 'output', cons2, 'input')

    comp = CompositePE(create_graph, {'connections': 2})
    comp._map_input('comp_input', cons1, 'input')
    comp._map_output('comp_output', cons2, 'output')
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', comp, 'comp_input')
    graph.connect(comp, 'comp_output', cons, 'input')
    graph.flatten()
    results = simple_process.process_and_return(graph, {prod: 10})
    expected = []
    for i in range(1, 11):
        expected += [i, i]
    tools.eq_({cons.id: {'output': expected}}, results)


def test_process():
    prod = TestProducer()
    cons = PrintDataConsumer()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons, 'input')
    simple_process.process(graph, inputs={prod: 5})


def test_process_input_by_id():
    prod = TestProducer()
    cons = PrintDataConsumer()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons, 'input')
    simple_process.process(graph, inputs={prod.id: 5})


def testWordCount():
    prod = RandomWordProducer()
    filt = RandomFilter()
    count = WordCounter()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', filt, 'input')
    graph.connect(filt, 'output', count, 'input')
    simple_process.process(graph, inputs={prod: 100})
