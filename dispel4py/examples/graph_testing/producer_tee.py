from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut
from dispel4py.workflow_graph import WorkflowGraph


prod = TestProducer()
cons1a = TestOneInOneOut()
cons1b = TestOneInOneOut()
cons2 = TestOneInOneOut()

graph = WorkflowGraph()
graph.connect(prod, 'output', cons1a, 'input')
graph.connect(prod, 'output', cons1b, 'input')
graph.connect(cons1a, 'output', cons2, 'input')