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
This graph is a modification of the
:py:mod:`~test.graph_testing.parallel_pipeline` example,
showing how the user can specify how the graph is going to be partitioned
into MPI processes.
In this example we are specifying that one MPI process is executing the
pipeline of nodes ``prod``, ``cons1`` and ``cons2`` and the other MPI
processes are executing the remaining node ``cons3``::

    graph.partitions = [ [prod, cons1, cons2], [cons3] ]


It can be executed with MPI and Storm. Storm will ignore the partition
information.

Execution:

* MPI: Please, locate yourself into the dispel4py directory.

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> dispel4py mpi\\
            [-a name_dispel4py_graph]\\
            [-f file containing the input dataset in JSON format]\\
            [-i number of iterations/runs']\\
            [-s]

    The argument '-s' forces to run the graph in a simple processing, which
    means that the first node of the graph will be executed in a process,
    and the rest of nodes will be executed in a second process.
    When [-i number of interations/runs] is not indicated, the graph is
    executed once by default.


    For example::

        mpiexec -n 3 python -m dispel4py.worker_mpi \\
            dispel4py.examples.graph_testing.partition_parallel_pipeline -i 10

    Output::

        Partitions:  [TestProducer0, TestOneInOneOut1, TestOneInOneOut2], \
[TestOneInOneOut3]
        Processes: {'GraphWrapperPE5': [0, 1], 'GraphWrapperPE4': [2]}
        GraphWrapperPE5 (rank 0): I'm a bolt
        GraphWrapperPE5 (rank 1): I'm a bolt
        GraphWrapperPE4 (rank 2): I'm a spout
        Rank 2: Sending terminate message to [0, 1]
        GraphWrapperPE4 (rank 2): Processed 10 input block(s)
        GraphWrapperPE4 (rank 2): Completed.
        GraphWrapperPE5 (rank 1): Processed 5 input block(s)
        GraphWrapperPE5 (rank 1): Completed.
        GraphWrapperPE5 (rank 0): Processed 5 input block(s)
        GraphWrapperPE5 (rank 0): Completed.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testParallelPipeline():
    '''
    Creates the parallel pipeline graph with partitioning information.

    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    cons3 = t.TestOneInOneOut()

    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons1, 'output', cons3, 'input')

    graph.partitions = [[prod, cons1, cons2], [cons3]]

    return graph

''' important: this is the graph_variable '''
graph = testParallelPipeline()
