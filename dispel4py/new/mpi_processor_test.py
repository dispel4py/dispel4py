from dispel4py.new.processor import MPIWrapper, ShuffleCommunication
from dispel4py.core import GenericPE
from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut

from mpi4py import MPI
        
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

if rank == 0:
    prod = TestProducer()
    communication = ShuffleCommunication(0, [0], [1])
    prod_wrapper = MPIWrapper(prod, [ { 'input' : 1 } ] )
    prod_wrapper.targets = { 'output' : [ ('input', communication)] }
    prod_wrapper.process()
if rank == 1:
    cons = TestOneInOneOut()
    cons_wrapper = MPIWrapper(cons)
    cons_wrapper.process()