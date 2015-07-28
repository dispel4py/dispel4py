from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE
from dispel4py.examples.graph_testing.testing_PEs \
    import TestProducer, TestOneInOneOut


class ExceptionRaiser(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, data):
        return data

    def _postprocess(self):
        raise Exception("Uncaught exception")


prod = TestProducer()
cons = ExceptionRaiser()
cons2 = TestOneInOneOut()

graph = WorkflowGraph()
graph.connect(prod, 'output', cons, 'input')
graph.connect(cons, 'output', cons2, 'input')
