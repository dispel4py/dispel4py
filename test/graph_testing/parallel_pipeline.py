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
This is a dispy graph where each MPI process computes a partition of the workflow instead of a PE instance. 
This happens automatically when the graph has more nodes than MPI processes. 
In terms of internal execution, the user has control which parts of the graph are distributed to each MPI process. 
See :py:mod:`~test.graph_testing.partition_parallel_pipeline` on how to specify the partitioning. 

.. image:: /api/images/parallel_pipeline.png

It can be executed with MPI and STORM.

* MPI: Please, locate yourself into the dispy directory. 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi <name_dispy_graph> <-f file containing the input dataset in JSON format>
	<-i number of iterations/runs'> <-s>
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When <-i number of interations/runs> is not indicated, the graph is executed once by default. 	
    
    For example::

    For example::
    
        mpiexec -n 3 python -m dispel4py.worker_mpi test.parallel_pipeline -i 10 
        
    .. note::
    
        To force the partitioning the graph must have more nodes than available MPI processes.
        This graph has 4 nodes and we use 3 MPI processes to execute it. Besides, if we use -s option, the graph will be partitioned only in 2 MPI processes.  
        
    Output::

		Processing 10 iterations
		Graph is too large for MPI job size: 4 > 3. Start simple processing.
		Partitions:  [TestProducer0], [TestOneInOneOut1, TestOneInOneOut2, TestOneInOneOut3]
		Processes: {'GraphWrapperPE5': [1, 2], 'GraphWrapperPE4': [0]}
		GraphWrapperPE4 (rank 0): I'm a spout
		GraphWrapperPE5 (rank 1): I'm a bolt
		Rank 0: Sending terminate message to [1, 2]
		GraphWrapperPE4 (rank 0): Processed 10 input block(s)
		GraphWrapperPE4 (rank 0): Completed.
		GraphWrapperPE5 (rank 1): Processed 5 input block(s)
		GraphWrapperPE5 (rank 1): Completed.
		GraphWrapperPE5 (rank 2): I'm a bolt
		GraphWrapperPE5 (rank 2): Processed 5 input block(s)
		GraphWrapperPE5 (rank 2): Completed.
				
* STORM:  
'''

from test.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph

def testParallelPipeline():
    '''
    Creates a graph with 4 nodes.
    
    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    prod = t.TestProducer()
    prev = prod
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    cons3 = t.TestOneInOneOut()

    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons1, 'output', cons3, 'input')

    return graph

''' important: this is the graph_variable '''
graph = testParallelPipeline()
