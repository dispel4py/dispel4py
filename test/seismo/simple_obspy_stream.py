from verce.workflow_graph import WorkflowGraph
from verce.GenericPE import GenericPE, NAME
from verce.seismo.seismo import SeismoPE
from verce.seismo.obspy_stream import createProcessingComposite, INPUT_NAME, OUTPUT_NAME
from test.graph_testing.testing_PEs import TestProducer, TestOneInOneOut
try:
    from obspy.core import stream
except ImportError:
    pass


def noop(stream):
    return stream
    
class StreamProducer(SeismoPE):
    OUTPUT_NAME = 'output'
    def __init__(self, numIterations=1):
        SeismoPE.__init__(self)
    def getDataStreams(self, inputs):
        self._timestamp = { 'starttime' : None, 'endtime' : None }
        self._location = { 'channel' : None, 'network' : None, 'station' : None }
        return {"streams": []}
    def compute(self):
        self.outputstreams = [ stream.Stream() ]
        
class StreamConsumer(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
    def process(self, inputs):
        try:
            if inputs['input'][2]['data']:
                self.log('Received stream')
        except:
            pass
        return None
    
chain = [ noop, noop, noop, noop ]
controlParameters = { 'runId' : '12345', 'username' : 'amyrosa',  'outputdest' : "./" }
composite = createProcessingComposite(chain, controlParameters=controlParameters)
producer = StreamProducer()
consumer = StreamConsumer()

graph = WorkflowGraph()
graph.connect(producer, StreamProducer.OUTPUT_NAME, composite, INPUT_NAME)
graph.connect(composite, OUTPUT_NAME, consumer, 'input')

