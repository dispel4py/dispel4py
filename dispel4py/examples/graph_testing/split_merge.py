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
This is a dispel4py graph which produces a workflow that splits the data and
sends it to two nodes (cons1 and cons2) and the output of those two nodes is
merged by another node (last).

.. image:: /images/split_merge.png

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

        mpiexec -n 4 dispel4py mpi dispel4py.examples.graph_testing.split_merge

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 4 nodes. For this reason we need at least 4 MPI
        processes to execute it.

    Output::

        Processing 1 iteration.
        Processes: {'TestProducer0': [1], 'TestOneInOneOutWriter2': [2], \
'TestTwoInOneOut3': [0], 'TestOneInOneOut1': [3]}
        TestProducer0 (rank 1): Processed 1 iteration.
        TestOneInOneOut1 (rank 3): Processed 1 iteration.
        TestOneInOneOutWriter2 (rank 2): Processed 1 iteration.
        TestTwoInOneOut3 (rank 0): Processed 2 iterations.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testSplitMerge():
    '''
    Creates the split/merge graph with 4 nodes.

    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    prod = t.TestProducer(2)
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOutWriter()
    last = t.TestTwoInOneOut()
    graph.connect(prod, 'output0', cons1, 'input')
    graph.connect(prod, 'output1', cons2, 'input')
    graph.connect(cons1, 'output', last, 'input0')
    graph.connect(cons2, 'output', last, 'input1')
    return graph

''' important: this is the graph_variable '''
graph = testSplitMerge()
