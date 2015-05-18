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
This is a dispel4py graph which produces a pipeline workflow with one producer
node (prod) and 5 consumer nodes.
It can be executed with MPI and STORM.

.. image:: /images/pipeline_test.png

Execution:

* MPI: Please, locate yourself into the dispel4py directory.

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> dispel4py mpi\\
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

        mpiexec -n 6 dispel4py mpi\\
            dispel4py.examples.graph_testing.pipeline_test

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 6 nodes.
        For this reason we need at least 6 MPI processes to execute it.

    Output::

        Processing 10 iterations.
        Processes: {'TestProducer0': [5], 'TestOneInOneOut5': [2], \
'TestOneInOneOut4': [4], 'TestOneInOneOut3': [3], 'TestOneInOneOut2': [1], \
'TestOneInOneOut1': [0]}
        TestProducer0 (rank 5): Processed 10 iterations.
        TestOneInOneOut1 (rank 0): Processed 10 iterations.
        TestOneInOneOut2 (rank 1): Processed 10 iterations.
        TestOneInOneOut3 (rank 3): Processed 10 iterations.
        TestOneInOneOut4 (rank 4): Processed 10 iterations.
        TestOneInOneOut5 (rank 2): Processed 10 iterations.

* STORM:

    From the dispel4py directory launch the Storm submission client::

        dispel4py storm dispel4py.examples.graph_testing.pipeline_test\\
            -m remote

    Output::

        Spec'ing TestOneInOneOut1
        Spec'ing TestOneInOneOut2
        Spec'ing TestOneInOneOut3
        Spec'ing TestOneInOneOut4
        Spec'ing TestOneInOneOut5
        Spec'ing TestProducer6
        spouts {'TestProducer6': ... }
        bolts  {'TestOneInOneOut5': ... }
        Created Storm submission package in \
/var/folders/58/7bjr3s011kgdtm5lx58prc_40000gn/T/tmp5ePEq3
        Running: java -client -Dstorm.options= -Dstorm.home= ...
        Submitting topology 'TestTopology' to storm.example.com:6627 ...

'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testPipeline(graph):
    '''
    Adds a pipeline to the given graph.

    :rtype: the created graph
    '''
    prod = t.TestProducer()
    prev = prod
    for i in range(5):
        cons = t.TestOneInOneOut()
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    return graph
''' important: this is the graph_variable '''
graph = testPipeline(WorkflowGraph())
