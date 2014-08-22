# Get to stage where all is xc-d tehn append a new taskfarm according to what 
# is to be done with the xc's.
# and look at data from the shm in memory files.
from dispel4py.core import GenericPE
from dispel4py.base import BasePE, IterativePE, ConsumerPE, create_iterative_chain
from dispel4py.seismo.simple import create_pipeline
from dispel4py.workflow_graph import WorkflowGraph

from whiten import spectralwhitening
from normalization import onebit_norm, mean_norm, gain_norm
import numpy as np
from obspy.core import read as obread

IN1="/Users/akrause/VERCE/data/Terracorrelator/A25A.TA..BHZ.2011.025.00.00.00.000-2011.026.00.00.39.000.rm.scale-AUTO.SAC"
IN2="/Users/akrause/VERCE/data/Terracorrelator/BMN.LB..BHZ.2011.025.00.00.00.023-2011.026.00.00.38.998.rm.scale-AUTO.SAC"
R1="/Users/akrause/VERCE/data/Terracorrelator/RESP.A25A.TA..BHZ"
R2="/Users/akrause/VERCE/data/Terracorrelator/RESP.BMN.LB..BHZ"

NREPEATS=1


class FindFiles(GenericPE):
    OUTPUT_NAME = 'output'

    def __init__(self, inf1, rf1, inf2,rf2):
        GenericPE.__init__(self)
        self.inf1=inf1
        self.rf1=rf1
        self.inf2=inf2
        self.rf2=rf2
        self._add_output(self.OUTPUT_NAME)
        self.count = 0
        
    def process(self, inputs):
        for i in range(NREPEATS):
            self.write(self.OUTPUT_NAME, [self.count, self.inf1, self.rf1])
            self.write(self.OUTPUT_NAME, [self.count+1, self.inf2, self.rf2])
            self.count += 2

class StreamProducer(GenericPE):
    OUTPUT_NAME = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self._add_output(self.OUTPUT_NAME)
        self.count = 0
    def process(self, inputs):
        for i in range(NREPEATS):
            self._read_stream(IN1, R1)
            self.log('Read stream %s' % IN1)
            self._read_stream(IN2, R2)
            self.log('Read stream %s' % IN2)
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
    def __init__(self, compute_fn, params):
        IterativePE.__init__(self)
        self.compute_fn = compute_fn
        self.params = params
    def _process(self, data):
        count=data[0]
        str1=data[1]
        rf=data[2]
        # this is an odd one - need to put one of the tuple values into the parameters
        # not sure how to handle this in general, hard coded for now
        try:
            self.params['seedresp']['filename'] = rf
        except KeyError:
            pass
        result = self.compute_fn(str1, **self.params)
        self.log('Done processing: %s' % self.compute_fn.__name__)
        return [count, result, rf]

class PreTask(GenericPE):
    OUTPUT_NAME_1 = 'output1'
    OUTPUT_NAME_2 = 'output2'
    INPUT_NAME = 'input'

    def __init__(self):
        GenericPE.__init__(self)
        self.outputconnections = { self.OUTPUT_NAME_1 : { NAME : self.OUTPUT_NAME_1 }, self.OUTPUT_NAME_2: {NAME : self.OUTPUT_NAME_2} }
        self.inputconnections = { self.INPUT_NAME : { NAME : self.INPUT_NAME } }

    def process(self, inputs):


        sps = 4 #Desired samples per second
        count=inputs[self.INPUT_NAME][0]
        str1=inputs[self.INPUT_NAME][1]
        str1.decimate(int(str1[0].stats.sampling_rate/sps))
        #self.log ("After decimate")

        # Phase 1C: Detrend and demean the data
        str1.detrend('simple')
        str1.detrend('demean')

        #self.log ("After detrend2")
# Phase 1d: Read in station response files and remove station responses - this seems a little slow, and what is the best "pre-filter"?
        pre_filt = (0.005, 0.006, 30.0, 35.0)

        rf=inputs[self.INPUT_NAME][2]     

        seedresp1 = {'filename': rf, 'units': 'VEL'}
        str1.simulate(paz_remove=None, pre_filt=pre_filt, seedresp=seedresp1)
        #self.log ("After simulate")
# Phase 1e: Apply bandpass filter
        str1.filter('bandpass', freqmin=0.01, freqmax=1., corners=4, zerophase=False)
        #Method 2 - moving-average normalization
        str1 = mean_norm(str1,15)
        #self.log ("After filter")

        # Phase 1g: Apply spectral whitening operation - 
        # `we might need to do some smoothing here
        str1w = spectralwhitening(str1)
        #self.log ("After white")

        fout_white = '/dev/shm/rosa/PRE2_WHITE/%s_pre.white.out' % (count)
        fout_nowhite = '/dev/shm/rosa/PRE2_NOWHITE/%s_pre.nowhite.out' % (count)
        str1.write(fout_white, format='SAC')
        str1w.write(fout_nowhite, format='SAC')
	#np.save(fout_white,str1)
	#np.save(fout_nowhite,str1w)
	#self.log("After writing files %s" % count)


# findfiles=FindFiles(IN1,R1,IN2,R2)
streamProducer=StreamProducer()
streamToFile = StreamToFile('/Users/akrause/VERCE/data/OUTPUT/NOWHITE/%s_preprocessed.SAC')
streamToFileWhitened = StreamToFile('/Users/akrause/VERCE/data/OUTPUT/WHITE/%s_preprocessed.SAC')
functions = [ 
                (decimate, { 'sps' : 4 }), 
                detrend, 
                demean, 
                (simulate, {'paz_remove' : None, 'pre_filt' : (0.005, 0.006, 30.0, 35.0), 'seedresp' : {'units': 'VEL'} }),
                (filter, {'freqmin':0.01, 'freqmax':1., 'corners':4, 'zerophase':False}),
                (mean_norm, { 'N' : 15 })
            ]
preTask = create_iterative_chain(functions, PreTaskPE)
whiten = PreTaskPE(spectralwhitening)

# preTask=PreTask()
# preTask.numprocesses=36

graph = WorkflowGraph()
# graph.connect(findfiles,'output',streamProducer, 'input')
graph.connect(streamProducer, StreamProducer.OUTPUT_NAME, preTask, 'input')
graph.connect(preTask, 'output', streamToFile, StreamToFile.INPUT_NAME)
graph.connect(preTask, 'output', whiten, IterativePE.INPUT_NAME)
