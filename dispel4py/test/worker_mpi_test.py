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

    mpiexec -n <num_proc> python -m dispel4py.worker_mpi dispel4py.test.worker_mpi_test -a <graph> [-i <iterations>]

where graph is the name of the graph to test, one of 'pipeline', 'square', 'tee' or 'twopipes', 
and the number of iterations is 1 by default.
'''

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.GenericPE import GenericPE, NAME

from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut, TestOneInOneOutWriter, TestTwoInOneOut

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
