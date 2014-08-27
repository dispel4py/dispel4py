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
This is a dispel4py graph which produces two pipeline workflows which are unconnected.  

.. image:: /api/images/unconnected_pipeline.png

It can be executed with MPI and STORM. 

* MPI: Please, locate yourself into the dispel4py directory. 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi [-a name_dispel4py_graph] [-f file containing the input dataset in JSON format]
	[-i number of iterations/runs'] [-s]
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When [-i number of interations/runs] is not indicated, the graph is executed once by default. 	
    
        

    For example::
    
        mpiexec -n 12 python -m dispel4py.worker_mpi dispel4py.examples.graph_testing.unconnected_pipeline 
        
    .. note::
    
        Each node in the graph is executed as a separate MPI process. 
        This graph has 12 nodes. For this reason we need at least 12 MPI processes to execute it.
        
    Output::

        Processing 1 iterations
        Processes: {'TestProducer0': [7], 'TestProducer6': [3], 'TestOneInOneOut9': [1], 'TestOneInOneOut8': [5], 'TestOneInOneOut7': [9], 'TestOneInOneOut5': [0], 'TestOneInOneOut4': [4], 'TestOneInOneOut3': [6], 'TestOneInOneOut2': [11], 'TestOneInOneOut1': [2], 'TestOneInOneOut11': [8], 'TestOneInOneOut10': [10]}
        TestOneInOneOut5 (rank 0): I'm a bolt
        TestOneInOneOut11 (rank 8): I'm a bolt
        TestOneInOneOut4 (rank 4): I'm a bolt
        TestOneInOneOut9 (rank 1): I'm a bolt
        TestOneInOneOut1 (rank 2): I'm a bolt
        TestOneInOneOut7 (rank 9): I'm a bolt
        TestProducer6 (rank 3): I'm a spout
        Rank 3: Sending terminate message to [9]
        TestProducer6 (rank 3): Processed 1 input block(s)
        TestProducer6 (rank 3): Completed.
        TestOneInOneOut8 (rank 5): I'm a bolt
        TestOneInOneOut2 (rank 11): I'm a bolt
        TestOneInOneOut3 (rank 6): I'm a bolt
        TestProducer0 (rank 7): I'm a spout
        Rank 7: Sending terminate message to [2]
        TestProducer0 (rank 7): Processed 1 input block(s)
        TestProducer0 (rank 7): Completed.
        TestOneInOneOut10 (rank 10): I'm a bolt
        Rank 9: Sending terminate message to [5]
        TestOneInOneOut7 (rank 9): Processed 1 input block(s)
        TestOneInOneOut7 (rank 9): Completed.
        Rank 2: Sending terminate message to [11]
        TestOneInOneOut1 (rank 2): Processed 1 input block(s)
        TestOneInOneOut1 (rank 2): Completed.
        Rank 5: Sending terminate message to [1]
        TestOneInOneOut8 (rank 5): Processed 1 input block(s)
        TestOneInOneOut8 (rank 5): Completed.
        Rank 11: Sending terminate message to [6]
        TestOneInOneOut2 (rank 11): Processed 1 input block(s)
        TestOneInOneOut2 (rank 11): Completed.
        Rank 6: Sending terminate message to [4]
        TestOneInOneOut3 (rank 6): Processed 1 input block(s)
        TestOneInOneOut3 (rank 6): Completed.
        Rank 1: Sending terminate message to [10]
        TestOneInOneOut9 (rank 1): Processed 1 input block(s)
        TestOneInOneOut9 (rank 1): Completed.
        Rank 4: Sending terminate message to [0]
        TestOneInOneOut4 (rank 4): Processed 1 input block(s)
        TestOneInOneOut4 (rank 4): Completed.
        TestOneInOneOut5 (rank 0): Processed 1 input block(s)
        TestOneInOneOut5 (rank 0): Completed.
        Rank 10: Sending terminate message to [8]
        TestOneInOneOut10 (rank 10): Processed 1 input block(s)
        TestOneInOneOut10 (rank 10): Completed.
        TestOneInOneOut11 (rank 8): Processed 1 input block(s)
        TestOneInOneOut11 (rank 8): Completed.
				
* STORM:  
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testPipeline(graph):
    '''
    Creates a pipeline and adds it to the given graph.
    
    :rtype: the modified graph
    '''
    prod = t.TestProducer()
    prev = prod
    part1 = [prod]
    part2 = []
    for i in range(5):
        cons = t.TestOneInOneOut()
        part2.append(cons)
        graph.connect(prev, 'output', cons, 'input')
        prev = cons
    return graph
    
def testUnconnected():
    '''
    Creates a graph with two unconnected pipelines.
    
    :rtype: the created graph
    '''
    graph = WorkflowGraph()
    testPipeline(graph)
    testPipeline(graph)
    return graph


''' important: this is the graph_variable '''
graph = testUnconnected()
