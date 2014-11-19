from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut

from simple_process import process
from dispel4py.workflow_graph import WorkflowGraph

prod = TestProducer()
cons1 = TestOneInOneOut()
cons2 = TestOneInOneOut()
graph = WorkflowGraph()
graph.connect(prod, 'output', cons1, 'input')
graph.connect(cons1, 'output', cons2, 'input')
process(graph, inputs={ prod : [ { 'input' : 1 }, { 'input' : 2 }, { 'input' : 3 }  ] } )