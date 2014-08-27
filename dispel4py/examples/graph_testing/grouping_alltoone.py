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
This is a dispel4py graph which produces a workflow with a pipeline 
in which the producer node ``prod`` sends data to the consumer node ``cons1`` which then sends data to node ``cons2``. 
Note that in this graph we have defined several instances of the cons1 and cons2 nodes
and all the instances of the cons1 node are sending data to only one instance of cons2 node. 
This type of grouping is called *global* in dispel4py (all to one). 

It can be executed with MPI and STORM.

* MPI: Please, locate yourself into the dispel4py directory. 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi <name_dispel4py_graph> <-f file containing the input dataset in JSON format>
	<-i number of iterations/runs'> <-s>
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When <-i number of interations/runs> is not indicated, the graph is executed once by default. 	

    For example::
    
        mpiexec -n 11 python -m dispel4py.worker_mpi dispel4py.examples.graph_testing.grouping_alltoone -i 10
        
    .. note::
    
        Each node in the graph is executed as a separate MPI process. 
        This graph has 11 nodes. For this reason we need at least 11 MPI processes to execute it. 
        
    Output::
			
        Processing 10 iterations
        Processes: {'TestProducer0': [5], 'TestOneInOneOut2': [6, 7, 8, 9, 10], 'TestOneInOneOut1': [0, 1, 2, 3, 4]}
        TestOneInOneOut1 (rank 0): I'm a bolt
        TestOneInOneOut1 (rank 1): I'm a bolt
        TestOneInOneOut2 (rank 9): I'm a bolt
        TestOneInOneOut1 (rank 2): I'm a bolt
        TestOneInOneOut1 (rank 4): I'm a bolt
        TestOneInOneOut1 (rank 3): I'm a bolt
        TestProducer0 (rank 5): I'm a spout
        TestOneInOneOut2 (rank 7): I'm a bolt
        Rank 5: Sending terminate message to [0, 1, 2, 3, 4]
        TestProducer0 (rank 5): Processed 10 input block(s)
        TestProducer0 (rank 5): Completed.
        TestOneInOneOut1 (rank 3): Processed 2 input block(s)
        TestOneInOneOut1 (rank 3): Completed.
        TestOneInOneOut1 (rank 1): Processed 2 input block(s)
        TestOneInOneOut1 (rank 1): Completed.
        TestOneInOneOut1 (rank 4): Processed 2 input block(s)
        TestOneInOneOut1 (rank 4): Completed.
        TestOneInOneOut1 (rank 2): Processed 2 input block(s)
        TestOneInOneOut1 (rank 2): Completed.
        Rank 0: Sending terminate message to [6, 7, 8, 9, 10]
        TestOneInOneOut1 (rank 0): Processed 2 input block(s)
        TestOneInOneOut1 (rank 0): Completed.
        TestOneInOneOut2 (rank 8): I'm a bolt
        TestOneInOneOut2 (rank 8): Processed 0 input block(s)
        TestOneInOneOut2 (rank 8): Completed.
        TestOneInOneOut2 (rank 9): Processed 0 input block(s)
        TestOneInOneOut2 (rank 9): Completed.
        TestOneInOneOut2 (rank 7): Processed 0 input block(s)
        TestOneInOneOut2 (rank 7): Completed.
        TestOneInOneOut2 (rank 10): I'm a bolt
        TestOneInOneOut2 (rank 10): Processed 0 input block(s)
        TestOneInOneOut2 (rank 10): Completed.
        TestOneInOneOut2 (rank 6): I'm a bolt
        TestOneInOneOut2 (rank 6): Processed 10 input block(s)
        TestOneInOneOut2 (rank 6): Completed.
        
    Note that only one instance of the consumer node ``TestOneInOneOut2`` (rank 6) received all the input blocks from the previous PE,
    as the grouping has been defined as `global`.
				
* STORM:  
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph

def testAlltoOne():
    '''
    Creates a graph with two consumer nodes and a global grouping.
    
    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    prod = t.TestProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    cons1.numprocesses=5
    cons2.numprocesses=5
    graph.connect(prod, 'output', cons1, 'input')
    cons2.inputconnections['input']['grouping'] = 'global'
    graph.connect(cons1, 'output', cons2, 'input')
    return graph

''' important: this is the graph_variable '''
graph = testAlltoOne()
