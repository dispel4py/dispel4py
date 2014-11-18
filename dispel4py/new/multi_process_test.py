from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut

# from dispel4py.new.processor import ShuffleCommunication
# from multiprocessing import Queue, Process
#
# def _processWorker(wrapper):
#     wrapper.process()
#
# conx = Queue()
#
# prod = TestProducer()
# communication = ShuffleCommunication(0, [0], [1])
# prod_wrapper = MultiProcessingWrapper(0, prod, [ { 'input' : 1 } ] )
# prod_wrapper.targets = { 'output' : [ ('input', communication)] }
# prod_wrapper.output_queues = { 1 : conx }
# prod_proc = Process(target=_processWorker, args=(prod_wrapper,))
# prod_proc.start()
#
# cons = TestOneInOneOut()
# cons_wrapper = MultiProcessingWrapper(1, cons)
# cons_wrapper.input_queue = conx
# cons_proc = Process(target=_processWorker, args=(cons_wrapper,))
# cons_proc.start()
#
# prod_proc.join()
# cons_proc.join()

from multi_process import process
from dispel4py.workflow_graph import WorkflowGraph

prod = TestProducer()
cons = TestOneInOneOut()
graph = WorkflowGraph()
graph.connect(prod, 'output', cons, 'input')
process(graph, size=2, inputs={ prod : [ { 'input' : 1 }, { 'input' : 2 }, { 'input' : 3 }  ] } )