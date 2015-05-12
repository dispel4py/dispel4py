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
This is a dispel4py graph which produces a pipeline workflow with one producer
node (prod) and 2 consumer nodes. The second consumer node delays the output by
a fixed time and records the average processing time.

Execution:

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
    When [-i number of interations/runs] is not indicated, the graph is
    executed once by default.


    For example::

        mpiexec -n 6 dispel4py mpi \\
            dispel4py.examples.graph_testing.delayed_pipeline

    .. note::

        Each node in the graph is executed as a separate MPI process.
        This graph has 3 nodes. For this reason we need at least 3 MPI
        processes to execute it.

    Output::

        Processes: {'TestDelayOneInOneOut2': [2, 3], 'TestProducer0': [4], \
'TestOneInOneOut1': [0, 1]}
        TestProducer0 (rank 4): Processed 10 iterations.
        TestOneInOneOut1 (rank 1): Processed 5 iterations.
        TestOneInOneOut1 (rank 0): Processed 5 iterations.
        TestDelayOneInOneOut2 (rank 3): Average processing time: 1.00058307648
        TestDelayOneInOneOut2 (rank 3): Processed 5 iterations.
        TestDelayOneInOneOut2 (rank 2): Average processing time: 1.00079641342
        TestDelayOneInOneOut2 (rank 2): Processed 5 iterations.

'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.new.monitoring import ProcessTimingPE

prod = t.TestProducer()
cons1 = t.TestOneInOneOut()
''' adding a processing timer '''
cons2 = ProcessTimingPE(t.TestDelayOneInOneOut())

''' important: this is the graph_variable '''
graph = WorkflowGraph()
graph.connect(prod, 'output', cons1, 'input')
graph.connect(cons1, 'output', cons2, 'input')
