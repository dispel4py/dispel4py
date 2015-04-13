# Copyright (c) The University of Edinburgh 2014-2015
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
Tests for multiprocessing mapping.

Using nose (https://nose.readthedocs.org/en/latest/) run as follows::

    $ nosetests dispel4py/new/multi_process_test.py
    ...
    ----------------------------------------------------------------------
    Ran 3 tests in 0.042s

    OK
'''
import argparse

from dispel4py.examples.graph_testing.testing_PEs\
    import TestProducer, TestOneInOneOut, TestTwoInOneOut
from dispel4py.workflow_graph import WorkflowGraph
from multi_process import process


args = argparse.Namespace
args.num = 5
args.simple = False


def testPipeline():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    args = argparse.Namespace
    args.num = 5
    args.simple = False
    process(graph, inputs={prod: [{}, {}, {}]}, args=args)


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
    args.num = 4
    process(graph, inputs={prod: [{}]}, args=args)


def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    args.num = 3
    process(graph, inputs={prod: [{}, {}, {}, {}, {}]}, args=args)
