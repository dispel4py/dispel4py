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
This is a dispel4py graph which produces a pipeline workflow with one producer node and a chain of functions that process the data.

Execution: 

* Simple processing:
    
    Execute the sequential mapping as follows::

        python -m dispel4py.simple_process dispel4py.examples.graph_testing.pipeline_composite [-i number of iterations]
    
    By default, if the number of iterations is not specified, the graph is executed once.
    
    For example::
    
        python -m dispel4py.simple_process dispel4py.examples.graph_testing.pipeline_composite 
        
    Output::
    
        adding addTwo to chain
        adding multiplyByFour to chain
        adding divideByTwo to chain
        adding subtract to chain
        Processing 1 iteration.
        Starting simple processing.
        Inputs: [{}]
        Results: [{('PE_subtract3', 'output'): [5]}]    
        

* MPI: 

    Execute the MPI mapping as follows::

        mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi [-a name_dispel4py_graph] [-f file containing the input dataset in JSON format]
	[-i number of iterations/runs'] [-s]
	
    The argument '-s' forces to run the graph in a simple processing, which means that the first node of the graph will be executed in a process, and the rest of nodes will be        executed in a second process.  
    When <-i number of interations/runs> is not indicated, the graph is executed once by default. 	
    
        
    For example::
    
        mpiexec -n 5 python -m dispel4py.worker_mpi dispel4py.examples.graph_testing.pipeline_composite 
        
    .. note::
    
        Each node in the graph is executed as a separate MPI process. 
        This graph has 5 nodes (4 function PEs and one producer). For this reason we need at least 5 MPI processes to execute it. 
        
    Output::

        Processes: {'PE_addTwo0': [2], 'PE_subtract3': [3], 'PE_divideByTwo2': [4], 'PE_multiplyByFour1': [1], 'TestProducer4': [0]}
        PE_addTwo0 (rank 2): Starting to process
        PE_multiplyByFour1 (rank 1): Starting to process
        PE_multiplyByFour1 (rank 1): I'm a bolt
        PE_addTwo0 (rank 2): I'm a bolt
        PE_subtract3 (rank 3): Starting to process
        PE_subtract3 (rank 3): I'm a bolt
        PE_divideByTwo2 (rank 4): Starting to process
        PE_divideByTwo2 (rank 4): I'm a bolt
        TestProducer4 (rank 0): Starting to process
        TestProducer4 (rank 0): I'm a spout
        Rank 0: Sending terminate message to [2]
        TestProducer4 (rank 0): Processed 1 input block(s)
        TestProducer4 (rank 0): Completed.
        Rank 2: Sending terminate message to [1]
        PE_addTwo0 (rank 2): Processed 1 input block(s)
        PE_addTwo0 (rank 2): Completed.
        Rank 1: Sending terminate message to [4]
        PE_multiplyByFour1 (rank 1): Processed 1 input block(s)
        PE_multiplyByFour1 (rank 1): Completed.
        Rank 4: Sending terminate message to [3]
        PE_divideByTwo2 (rank 4): Processed 1 input block(s)
        PE_divideByTwo2 (rank 4): Completed.
        PE_subtract3 (rank 3): Processed 1 input block(s)
        PE_subtract3 (rank 3): Completed.
        
* STORM:  

    From the dispel4py directory launch the Storm submission client::
    
        python storm_submission.py dispel4py.examples.graph_testing.pipeline_composite -m remote
                
'''


from dispel4py.base import create_iterative_chain
from dispel4py.examples.graph_testing.testing_PEs import TestProducer
from dispel4py.workflow_graph import WorkflowGraph

def addTwo(data):
    '''
    Returns 2 + `data`.
    '''
    return 2 + data
    
def multiplyByFour(data):
    '''
    Returns 4 * `data`.
    '''
    return 4 * data

def divideByTwo(data):
    '''
    Returns `data`/2.
    '''
    return data/2

def subtract(data, n):
    '''
    Returns `data` - `n`.
    '''
    return data - n
    
functions = [ addTwo, multiplyByFour, divideByTwo, (subtract, { 'n' : 1 }) ]
composite = create_iterative_chain(functions)
producer = TestProducer()

graph = WorkflowGraph()
graph.connect(producer, 'output', composite, 'input')