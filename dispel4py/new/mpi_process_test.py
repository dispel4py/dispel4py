from dispel4py.new.mpi_process import process
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut

from mpi4py import MPI
        
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

prod = TestProducer()
cons1 = TestOneInOneOut()
cons2 = TestOneInOneOut()
    
graph = WorkflowGraph()
graph.connect(prod, 'output', cons1, 'input')
graph.connect(cons1, 'output', cons2, 'input')

process(graph, { prod : [ {}, {}, {} ] } )
