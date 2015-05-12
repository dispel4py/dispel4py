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
This is a dispel4py graph which produces a workflow that sends copies of the
output data from the producer node (words) to two nodes (filter1 and filter2),
and the outputs of those two filters are merged in the last node (count).

.. image:: /images/grouping_split_merge.png

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

        mpiexec -n 4 python -m dispel4py.worker_mpi\\
            dispel4py.examples.graph_testing.grouping_split_merge


    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 4 nodes. For this reason we need at least 4 MPI
        processes to execute it.

    Output::

        Processing 1 iterations
        Processes: {'RandomFilter2': [3], 'WordCounter3': [1], \
'RandomFilter1': [0], 'RandomWordProducer0': [2]}
        RandomFilter1 (rank 0): I'm a bolt
        RandomWordProducer0 (rank 2): I'm a spout
        WordCounter3 (rank 1): I'm a bolt
        Rank 2: Sending terminate message to [0]
        Rank 2: Sending terminate message to [3]
        RandomWordProducer0 (rank 2): Processed 1 input block(s)
        RandomWordProducer0 (rank 2): Completed.
        RandomFilter2 (rank 3): I'm a bolt
        Rank 3: Sending terminate message to [1]
        RandomFilter2 (rank 3): Processed 1 input block(s)
        RandomFilter2 (rank 3): Completed.
        Rank 0: Sending terminate message to [1]
        RandomFilter1 (rank 0): Processed 1 input block(s)
        RandomFilter1 (rank 0): Completed.
        WordCounter3 (rank 1): Processed 2 input block(s)
        WordCounter3 (rank 1): Completed.

    .. note::

        As those PEs are filtering randomly the output could be completely
        different.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testGrouping():
    '''
    Creates the test graph.
    '''
    words = t.RandomWordProducer()
    filter1 = t.RandomFilter()
    filter2 = t.RandomFilter()
    count = t.WordCounter()
    graph = WorkflowGraph()
    graph.connect(words, 'output', filter1, 'input')
    graph.connect(words, 'output', filter2, 'input')
    graph.connect(filter1, 'output', count, 'input')
    graph.connect(filter2, 'output', count, 'input')

    return graph

''' important: this is the graph_variable '''
graph = testGrouping()
