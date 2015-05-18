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
This is a dispel4py graph which produces a workflow that copies the data
(from node prod) to two nodes (cons1 and cons2).
If you compare this graph with :py:mod:`~test.graph_testing.grouping_onetoall`
they look quite similar. However, they do different things.
In this example, the nodes ``cons1`` and ``cons2`` are different PE and the
node ``prod`` sends the same data to both PEs.

.. image:: /images/teecopy.png

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

        mpiexec -n 3 dispel4py mpi dispel4py.examples.graph_testing.teecopy

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 3 nodes. For this reason we need at least 3 MPI
        processes to execute it.

    Output::

        Processing 1 iteration.
        Processes: {'TestProducer0': [0], 'TestOneInOneOut2': [2], \
'TestOneInOneOut1': [1]}
        TestProducer0 (rank 0): Processed 1 iteration.
        TestOneInOneOut2 (rank 2): Processed 1 iteration.
        TestOneInOneOut1 (rank 1): Processed 1 iteration.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testTee():
    '''
    Creates a graph with two consumer nodes and a tee connection.

    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    return graph

''' important: this is the graph_variable '''
graph = testTee()
