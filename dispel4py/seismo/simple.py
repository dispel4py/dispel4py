from dispel4py.base import IterativePE

class SeismoStreamPE(IterativePE):
    
    INPUT_NAME = IterativePE.INPUT_NAME
    OUTPUT_NAME = IterativePE.OUTPUT_NAME
    
    def __init__(self, compute_fn, params={}):
        IterativePE.__init__(self)
        self.compute_fn = compute_fn
        self.params = params
        
    def _reset(self):
        self._inputs = None
        self._input_tuple = None
        self._data = None
        self._metadata = None
        self._timestamp = None
        self._location = None
        
    def _assign(self, inputs):
        self._inputs = inputs[SeismoStreamPE.INPUT_NAME]
        self._input_tuple = self._inputs[2:]
        self._data = [ d.data for d in self._input_data ]
        self._metadata = [ d.metadata for d in self._input_data ]
        self._timestamp = self._inputs[0]
        self._location = self._inputs[1]
        self._output_tuple = []
        
    def get_data(self):
        return self._input_data
            
    def write_data(self, data, metadata=None):
        output = [self._timestamp, self._location, DataTuple(data, metadata)]
        self.write(SeismoStreamPE.OUTPUT_NAME, output)
            
    def process(self, inputs):
        try:
            self._assign(inputs)
            result = compute_fn(self, **params)
            if result:
                self.write_data(result)
        finally:
            self._reset()
        
class DataTuple(object):
    def __init__(self, data=None, metadata=None):
        self.data = data
        self.metadata = metadata

from dispel4py.workflow_graph import WorkflowGraph

def create_pipeline(chain, name_prefix='SeismoStreamPE_', name_suffix=''):
    '''
    Creates a composite PE wrapping a pipeline that processes obspy streams.
    :param chain: list of functions that process obspy streams. The function takes one input parameter, stream, and returns an output stream.
    :param requestId: id of the request that the stream is associated with
    :param controlParameters: environment parameters for the processing elements
    :rtype: dictionary inputs and outputs of the composite PE that was created
    '''
    prev = None
    first = None
    graph = WorkflowGraph()
    
    for fn_desc in chain:
        try:
        	fn = fn_desc[0]
        	params = fn_desc[1]
        except TypeError:
            fn = fn_desc
            params = {}
            
        pe = SeismoStreamPE(fn, params)
        pe.name = name_prefix + fn.__name__ + name_suffix
        
        if prev:
            graph.connect(prev, SeismoStreamPE.OUTPUT_NAME, pe, SeismoStreamPE.INPUT_NAME)
        else:
            first = pe
        prev = pe
            
    # Map inputs and outputs of the wrapper to the nodes in the subgraph
    graph.inputmappings =  { 'input'  : (first, INPUT_NAME) }
    graph.outputmappings = { 'output' : (prev, OUTPUT_NAME) }
    
    return graph

