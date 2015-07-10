from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.base import IterativePE
from dispel4py.examples.graph_testing.testing_PEs import TestProducer


class ExceptionRaiser(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, data):
        if data % 2:
            raise Exception('Not an even number!')
        return data


class UndefinedOutput(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, data):
        self.write('does_not_exist', data)


prod = TestProducer()
cons = ExceptionRaiser()
undef = UndefinedOutput()

graph = WorkflowGraph()
graph.connect(prod, 'output', cons, 'input')
graph.connect(cons, 'output', undef, 'input')
