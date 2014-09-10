'''
Reads a number of SAC files and applies a preprocessing chain to the traces.

Run with::

    mpiexec -n 2 python -m dispel4py.worker_mpi dispel4py.examples.seismo.preprocess_example -a graph

or::

    python -m dispel4py.simple_process dispel4py.examples.seismo.preprocess_example -a graph
    
'''

from dispel4py.core import GenericPE
from dispel4py.base import BasePE, IterativePE, ConsumerPE, create_iterative_chain
from dispel4py.workflow_graph import WorkflowGraph

from whiten import spectralwhitening
from normalization import onebit_norm, mean_norm, gain_norm
import numpy as np
from obspy.core import read as obread

# These are the file locations
ROOT_DIR = '/Users/akrause/VERCE/data/Terracorrelator/'
IN1=ROOT_DIR + "A25A.TA..BHZ.2011.025.00.00.00.000-2011.026.00.00.39.000.rm.scale-AUTO.SAC"
IN2=ROOT_DIR + "BMN.LB..BHZ.2011.025.00.00.00.023-2011.026.00.00.38.998.rm.scale-AUTO.SAC"
R1=ROOT_DIR + "RESP.A25A.TA..BHZ"
R2=ROOT_DIR + "RESP.BMN.LB..BHZ"

# Modify this for larger test runs - each file above is repeated
NREPEATS=1

class StreamProducer(GenericPE):
    OUTPUT_NAME = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output(self.OUTPUT_NAME)
        self.count = 0
    def process(self, inputs):
        for i in range(NREPEATS):
            self._read_stream(IN1, R1)
            # self.log('Read stream %s' % IN1)
            self._read_stream(IN2, R2)
            # self.log('Read stream %s' % IN2)
    def _read_stream(self, inf, rf):
        st = obread(inf, format='SAC')
        self.write(self.OUTPUT_NAME, [self.count, st, rf])
        self.count += 1

class StreamToFile(ConsumerPE):
    def __init__(self, file_dest):
        ConsumerPE.__init__(self)
        self.file_dest = file_dest
    def _process(self, data):
        count=data[0]
        str1=data[1]
        try:
            fout = self.file_dest % count
        except TypeError:
            # maybe there's no "%s" in the string so we ignore count - bad idea?
            fout = file_dest
        str1.write(fout, format='SAC')
        self.log('Wrote file to %s' % fout)

def decimate(str1, sps):
    str1.decimate(int(str1[0].stats.sampling_rate/sps))
    return str1
def detrend(str1):
    str1.detrend('simple')
    return str1
def demean(str1):
    str1.detrend('demean')
    return str1
def simulate(str1, paz_remove, pre_filt, seedresp):
    str1.simulate(paz_remove=None, pre_filt=pre_filt, seedresp=seedresp)
    return str1
def filter(str1, freqmin=0.01, freqmax=1., corners=4, zerophase=False):
    str1.filter('bandpass', freqmin=freqmin, freqmax=freqmax, corners=corners, zerophase=zerophase)
    return str1
    
class PreTaskPE(IterativePE):
    def __init__(self, compute_fn, params={}):
        IterativePE.__init__(self)
        self.compute_fn = compute_fn
        self.params = params
    def _process(self, data):
        count=data[0]
        str1=data[1]
        rf=data[2]
        # this is an odd one - need to put one of the tuple values into the parameters list
        # not sure how to handle this in general, hard coded for now
        try:
            self.params['seedresp']['filename'] = rf
        except KeyError:
            pass
        result = self.compute_fn(str1, **self.params)
        self.log('%s: done processing: %s' % (count, self.compute_fn.__name__))
        return [count, result, rf]


streamProducer=StreamProducer()
streamToFile = StreamToFile(ROOT_DIR + 'OUTPUT/NOWHITE/%s_preprocessed.SAC')
streamToFile.name='StreamToFileNonWhitened'
streamToFileWhitened = StreamToFile(ROOT_DIR + 'OUTPUT/WHITE/%s_preprocessed.SAC')
streamToFileWhitened.name='StreamToFileWhitened'
functions = [ 
                (decimate, { 'sps' : 4 }), 
                detrend, 
                demean, 
                (simulate, {'paz_remove' : None, 
                            'pre_filt' : (0.005, 0.006, 30.0, 35.0), 
                            'seedresp' : {'units': 'VEL'} }),
                (filter, {'freqmin':0.01, 'freqmax':1., 'corners':4, 'zerophase':False}),
                (mean_norm, { 'N' : 15 })
            ]
preTask = create_iterative_chain(functions, PreTaskPE)
whiten = PreTaskPE(spectralwhitening)
whiten.name = 'PE_whiten'

graph = WorkflowGraph()
graph.connect(streamProducer, StreamProducer.OUTPUT_NAME, preTask, 'input')
graph.connect(preTask, 'output', streamToFile, StreamToFile.INPUT_NAME)
graph.connect(preTask, 'output', whiten, IterativePE.INPUT_NAME)
graph.connect(whiten, IterativePE.OUTPUT_NAME, streamToFileWhitened, StreamToFile.INPUT_NAME)
