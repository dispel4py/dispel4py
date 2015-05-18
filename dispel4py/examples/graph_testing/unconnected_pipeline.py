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
This is a dispel4py graph which produces two pipeline workflows which are
unconnected.

.. image:: /images/unconnected_pipeline.png

It can be executed with MPI and STORM.

* MPI: Please, locate yourself into the dispel4py directory.

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> dispel4py mpi <module|module file>\\
            [-a name_dispel4py_graph]\\
            [-f file containing the input dataset in JSON format]\\
            [-i number of iterations/runs']\\
            [-s]

    The argument '-s' forces to run the graph in a simple processing, which
    means that the first node of the graph will be executed in a process, and
    the rest of nodes will be executed in a second process.
    When [-i number of interations/runs] is not indicated, the graph is
    executed once by default.

    For example::

        mpiexec -n 12 dispel4py mpi \\
            dispel4py.examples.graph_testing.unconnected_pipeline

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 12 nodes. For this reason we need at least 12 MPI
        processes to execute it.

    Output::

        Processing 1 iteration.
        Processes: {'TestProducer0': [9], 'TestProducer6': [6], \
'TestOneInOneOut9': [7], 'TestOneInOneOut8': [8], 'TestOneInOneOut7': [11], \
'TestOneInOneOut5': [1], 'TestOneInOneOut4': [2], 'TestOneInOneOut3': [4], \
'TestOneInOneOut2': [0], 'TestOneInOneOut1': [10], 'TestOneInOneOut11': [3], \
'TestOneInOneOut10': [5]}
        TestProducer6 (rank 6): Processed 1 iteration.
        TestProducer0 (rank 9): Processed 1 iteration.
        TestOneInOneOut1 (rank 10): Processed 1 iteration.
        TestOneInOneOut7 (rank 11): Processed 1 iteration.
        TestOneInOneOut2 (rank 0): Processed 1 iteration.
        TestOneInOneOut8 (rank 8): Processed 1 iteration.
        TestOneInOneOut9 (rank 7): Processed 1 iteration.
        TestOneInOneOut10 (rank 5): Processed 1 iteration.
        TestOneInOneOut3 (rank 4): Processed 1 iteration.
        TestOneInOneOut4 (rank 2): Processed 1 iteration.
        TestOneInOneOut11 (rank 3): Processed 1 iteration.
        TestOneInOneOut5 (rank 1): Processed 1 iteration.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testPipeline(graph):
    '''
    Creates a pipeline and adds it to the given graph.

    :rtype: the modified graph
    '''
    prod = t.TestProducer()
    prev = prod
    for i in range(5):
        cons = t.TestOneInOneOut()
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    return graph


def testUnconnected():
    '''
    Creates a graph with two unconnected pipelines.

    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    testPipeline(graph)
    testPipeline(graph)
    return graph


''' important: this is the graph_variable '''
graph = testUnconnected()
