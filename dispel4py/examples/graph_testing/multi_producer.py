from dispel4py.examples.graph_testing.testing_PEs\
    import TestMultiProducer, TestOneInOneOut
from dispel4py.workflow_graph import WorkflowGraph


prod = TestMultiProducer()
cons1 = TestOneInOneOut()
cons2 = TestOneInOneOut()

graph = WorkflowGraph()
graph.connect(prod, 'output', cons1, 'input')
graph.connect(cons1, 'output', cons2, 'input')
