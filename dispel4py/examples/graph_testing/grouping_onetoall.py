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
This is a dispel4py graph which produces a workflow which copies the data
(from node ``prod``) to one node (``cons``).

Note, that in this graph, we have decided to assign two processes to instances
of the same PE::

    cons = t.TestOneInOneOut()
    cons.numprocesses=2

Another interesting point in this example is how to define the different types
of groupings. In this example we have::

    cons.inputconnections['input']['grouping'] = 'all'

which means that the ``prod`` node sends copies of its output data to *all* the
connected instances.
For that reason, the node ``cons`` has to specify the input grouping 'all' to
the connection.
In :py:mod:`~test.graph_testing.grouping_split_merge`, we use another type of
grouping, which is group by. However, in that case, the grouping type is
defined by the PE class (WordCounter) which means it applies to all instances
of that class (unless it is explicitly overridden by an instance as we did
above).

If you compare this graph with :py:mod:`~test.graph_testing.teecopy` these
look quite similar. However, they do different things.
In this example, we have two instances of the same PE and the node ``prod``
sends the same data to both instances whereas in
:py:mod:`~test.graph_testing.teecopy` the node ``prod`` sends the same data
to two different PEs.

It can be executed with MPI and STORM.

* MPI: Please, locate yourself into the dispel4py directory.

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi\\
            [-a name_dispel4py_graph]\\
            [-f file containing the input dataset in JSON format]\\
            [-i number of iterations/runs']\\
            [-s]

    The argument '-s' forces to run the graph in a simple processing, which
    means that the first node of the graph will be executed in a process, and
    the rest of nodes will be executed in a second process.
    When <-i number of interations/runs> is not indicated, the graph is
    executed once by default.


    For example::

        mpiexec -n 3 python -m dispel4py.worker_mpi\\
            dispel4py.examples.graph_testing.grouping_onetoall

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 3 nodes. For this reason we need at least 3 MPI
        processes to execute it.

    Output::

        Processing 1 iterations
        Processes: {'TestProducer0': [2], 'TestOneInOneOut1': [0, 1]}
        TestOneInOneOut1 (rank 0): I'm a bolt
        TestOneInOneOut1 (rank 1): I'm a bolt
        TestProducer0 (rank 2): I'm a spout
        Rank 2: Sending terminate message to [0, 1]
        TestProducer0 (rank 2): Processed 1 input block(s)
        TestProducer0 (rank 2): Completed.
        TestOneInOneOut1 (rank 1): Processed 1 input block(s)
        TestOneInOneOut1 (rank 1): Completed.
        TestOneInOneOut1 (rank 0): Processed 1 input block(s)
        TestOneInOneOut1 (rank 0): Completed.


* STORM:
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testOnetoAll():
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons = t.TestOneInOneOut()
    cons.numprocesses = 2
    cons.inputconnections['input']['grouping'] = 'all'
    graph.connect(prod, 'output', cons, 'input')
    return graph


''' important: this is the graph_variable '''
graph = testOnetoAll()
