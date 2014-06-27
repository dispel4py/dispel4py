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
This is a dispy graph which produces a workflow that splits the data and sends it to two nodes (cons1 and cons2)
and the output of those two nodes is merged by another node (last). 

.. image:: /api/images/split_merge.png

It can be executed with MPI and STORM. 

* MPI: Please, locate yourself into the dispy directory. 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi <name_dispy_graph> <-f file containing the input dataset in JSON format>
	<-i number of iterations/runs'> <-s>
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When <-i number of interations/runs> is not indicated, the graph is executed once by default. 	
        

    For example::
    
        mpiexec -n 4 python -m dispel4py.worker_mpi test.split_merge
        
    .. note::
    
        Each node in the graph is executed as a separate MPI process. 
        This graph has 4 nodes. For this reason we need at least 4 MPI processes to execute it. 
        
    Output::
    
        Processing 1 iterations
        Processes: {'TestProducer0': [0], 'TestOneInOneOutWriter2': [1], 'TestOneInOneOut1': [3], 'TestTwoInOneOut3': [2]}
        TestProducer0 (rank 0): I'm a spout
        Rank 0: Sending terminate message to [3]
        Rank 0: Sending terminate message to [1]
        TestProducer0 (rank 0): Processed 1 input block(s)
        TestProducer0 (rank 0): Completed.
        TestOneInOneOutWriter2 (rank 1): I'm a bolt
        Rank 1: Sending terminate message to [2]
        TestOneInOneOutWriter2 (rank 1): Processed 1 input block(s)
        TestOneInOneOutWriter2 (rank 1): Completed.
        TestTwoInOneOut3 (rank 2): I'm a bolt
        TestOneInOneOut1 (rank 3): I'm a bolt
        Rank 3: Sending terminate message to [2]
        TestOneInOneOut1 (rank 3): Processed 1 input block(s)
        TestOneInOneOut1 (rank 3): Completed.
        TestTwoInOneOut3 (rank 2): Processed 2 input block(s)
        TestTwoInOneOut3 (rank 2): Completed.
				
* STORM:  
'''

from test.graph_testing import testing_PEs as t
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
