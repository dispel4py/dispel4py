from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.examples.graph_testing.testing_PEs \
    import TestProducer, TestOneInOneOut

graph = WorkflowGraph()
prod = TestProducer()
cons1 = TestOneInOneOut()
cons2 = TestOneInOneOut()
cons3 = TestOneInOneOut()
cons4 = TestOneInOneOut()

graph.connect(prod, 'output', cons1, 'input')
graph.connect(prod, 'output', cons2, 'input')
graph.connect(cons1, 'output', cons3, 'input')
graph.connect(cons3, 'output', cons4, 'input')
graph.connect(cons4, 'output', cons2, 'input')
