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
This is a dispel4py graph which produces a workflow that copies the data (from node prod) to two nodes (cons2 and cons3). 
This example can be used to process a large number of data blocks for testing.
    
It can be executed with MPI and STORM.

* MPI: Please, locate yourself into the dispel4py directory. 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi [-a name_dispel4py_graph] [-f file containing the input dataset in JSON format]
	[-i number of iterations/runs'] [-s]
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When [-i number of interations/runs] is not indicated, the graph is executed once by default. 	
    Where <number_of_blocks> is the number of blocks produced by the source PE in each iteration.
    

    For example::
    
        mpiexec -n 4 python -m dispel4py.worker_mpi dispel4py.examples.graph_testing.large_teecopy 1000000
        
    .. note::
    
        Each node in the graph is executed as a separate MPI process. 
        This graph has 4 nodes. For this reason we need at least 4 MPI processes to execute it. 
        
    Output::

        Processing 1 iterations
        Processes: {'TestProducer0': [0], 'TestOneInOneOut2': [2], 'TestOneInOneOut1': [1]}
        TestProducer0 (rank 0): I'm a spout
        Rank 0: Sending terminate message to [1]
        Rank 0: Sending terminate message to [2]
        TestProducer0 (rank 0): Processed 1 input block(s)
        TestProducer0 (rank 0): Completed.
        TestOneInOneOut2 (rank 2): I'm a bolt
        TestOneInOneOut2 (rank 2): Processed 1 input block(s)
        TestOneInOneOut2 (rank 2): Completed.
        TestOneInOneOut1 (rank 1): I'm a bolt
        TestOneInOneOut1 (rank 1): Processed 1 input block(s)
        TestOneInOneOut1 (rank 1): Completed
				
* STORM:  
'''

import sys
from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph

def testTee():
    '''
    Creates a graph with two consumer nodes and a tee connection.
    
    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    try:
        numIterations = int(sys.argv[4])
    except:
        numIterations = 1
    prod = t.NumberProducer(numIterations)
    prev = prod
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    cons3 = t.TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons1, 'output', cons3, 'input')
    return graph

''' important: this is the graph_variable '''
graph = testTee()
