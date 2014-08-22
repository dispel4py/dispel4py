from dispel4py.workflow_graph import WorkflowGraph

from simple_PEs import StreamProducer, DetrendPE

prod = StreamProducer()
detrend = DetrendPE()

graph = WorkflowGraph()
graph.connect(prod, 'output', detrend, 'input')
