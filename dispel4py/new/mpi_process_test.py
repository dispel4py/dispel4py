from dispel4py.new import processor
from dispel4py.new.mpi_process import MPIWrapper, process
from dispel4py.core import GenericPE
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut

from mpi4py import MPI
        
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

prod = TestProducer()
cons = TestOneInOneOut()
# if rank == 0:
#     communication = processor.ShuffleCommunication(0, [0], [1])
#     prod_wrapper = MPIWrapper(prod, [ { 'input' : 1 } ] )
#     prod_wrapper.targets = { 'output' : [ ('input', communication)] }
#     prod_wrapper.process()
# if rank == 1:
#     cons_wrapper = MPIWrapper(cons)
#     cons_wrapper.process()
    
graph = WorkflowGraph()
graph.connect(prod, 'output', cons, 'input')

if rank == 0:
    success, sources, processes = processor._assign_processes(graph, size)
    print sources
    print processes
    inputmappings, outputmappings = processor._connect(graph, processes)
    print inputmappings
    print outputmappings

process(graph, { prod : [ { 'input' : 1 }, { 'input' : 2 }, { 'input' : 3 }  ] } )
