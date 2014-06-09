'''
Created on May 9, 2014
Modified on May 19, 2014

@author: mdavid@ipgp.fr
@author: rosa.filgueira@ed.ac.uk
@author: a.krause@epcc.ed.ac.uk

This program produces a graph which reads a file as an obspy stream. 
The name of the file is specified in the input file test/seismo/XCorrInput.json.
In case that the user wants to specify more than one input file have a look at test/seismo/XCorrInput-timestamps.json
where we have specified a list of input files to read.

 mpiexec -n 8 python -m verce.worker_mpi test.seismo.graphXCorr -s -f test/seismo/XCorrInput-timestamps.json 
'''

import scipy.signal
import h5py
import datetime
import time
import os
import traceback

from verce.workflow_graph import WorkflowGraph
from verce.GenericPE import GenericPE, NAME
from verce.seismo.seismo import SeismoPE
from verce.seismo.obspy_stream import createProcessingComposite, INPUT_NAME, OUTPUT_NAME
from test.graph_testing.testing_PEs import ProvenanceLogger

# Import here all PEs
from test.graph_testing.refWF_PEs import PEMeanSub, PEDetrend, PEGapCorr, PEClip, PEWhite, PEDecim
#from test.graph_testing.refWF_PEs import PEMeanSub, PEDetrend, PEGapCorr, PEClip, PEDecim

try:
    from obspy.core import read, trace, stream, UTCDateTime
except ImportError:
    pass

class FindFiles(GenericPE):
    INPUT_NAME = 'file'
    OUTPUT_NAME = 'output'

    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
        self.outputconnections = { 'output' : {NAME : 'output'} }

    def process(self, inputs):
        parameters = inputs['input']
        starttime = datetime.datetime.strptime(parameters['starttime'], '%Y%m%d')
        endtime = datetime.datetime.strptime(parameters['endtime'], '%Y%m%d')
        station = parameters['station']
        delta = datetime.timedelta(days=1)
        day = starttime
        while day <= endtime:
            filename = '%s/%s/%s.fseed' % (parameters['rootdir'], station, day.strftime('%Y%m%d'))
            self.log('Writing out filename = %s' % filename)
            self.write('output', filename)
            day = day + delta

class StreamProducer(SeismoPE):
    INPUT_NAME = 'file'
    OUTPUT_NAME = 'output'

    def __init__(self, numIterations=1):
        SeismoPE.__init__(self)
        self.inputconnections = { 'file' : { NAME : 'file'}}

    def getDataStreams(self, inputs):
        '''This method returns a stream with the filename.
        This method is called by the seismo framework to read the input data.
        '''
        self._timestamp = { 'starttime' : None, 'endtime' : None }
        self._location = { 'channel' : None, 'network' : None, 'station' : None }
        inputStr = inputs['file']
        self.attr =  {'network': None, 'channel': None, 'station': None,
                 'location': None, 'starttime': None, 'endtime': None,
                 'sampling_rate': None, 'npts': None}
        return {"streams": [{'data': inputStr, 'attr' : self.attr}]}

    def compute(self):
        '''This method reads the file and produces a stream.
        This method is called by the seismo framework to iniate processing.
        The input data is available as "self.st" or, if the input data is a list, it is stored in "self.streams". 
        In this example, there is only one piece of input data, the filename, and it is stored in "self.st".
        Compute is called for each input block, so if there are n filenames this method is called n times.
        '''
        ti = time.time()
        stream=read(self.st)
        stream.merge(fill_value=0) # fills gaps with 0, gap correction should be implemented as PE
        outputattr = dict(self.attr)
        outputattr['starttime'] = str(stream[0].stats.starttime)
        outputattr['endtime'] = str(stream[0].stats.endtime)
        outputattr['sampling_rate'] = stream[0].stats.sampling_rate
        outputattr['npts'] = stream[0].stats.npts
        outputattr['location'] = stream[0].stats.location
        outputattr['network'] = stream[0].stats.network
        outputattr['channel'] = stream[0].stats.channel
        outputattr['station'] = stream[0].stats.station

        self.outputattr=[outputattr]
        tf = time.time()
        dt = tf-ti
        self.log('='*120)
        self.log('PRODUCER dt = %f\tTSTAMP = %f\tTRACE = %s %s' %(dt, tf, stream[0].stats.station, stream[0].stats.starttime))
        self.log('Read stream: %s' % stream)
        self.log('='*120)
        self.outputstreams = [stream[0].data] # stream[0].data is a numpy masked array
        
class StreamConsumer(SeismoPE):
    def __init__(self):
        SeismoPE.__init__(self)

    def compute(self):
        '''This method is going to write the input stream to a file.'''
        fout = self.__getFileOut__()
        ti = time.time()
        with h5py.File(fout, 'w') as fdout:
            dset = fdout.create_dataset('seism', data=self.st)
        tf = time.time()
        dt = tf-ti
        self.log('='*120)
        self.log('CONSUMER dt = %f\tTSTAMP = %f\tTRACE = %s %s' %(dt, tf, self.attr['station'], self.attr['starttime']))
        self.log('OUT stream fname = %s' % fout)
        self.log('='*120)

        # Alternative write ouput
        # in case the input is a stream you can write a file like this:
        #self.st.write('outTest2.mseed', format='MSEED', encoding='FLOAT64')
        #with open('OUTPUT_TEST.txt', 'w') as f:
        #    f.write(self.st)

    def __getFileOut__(self):
        #rdir = '/data1/datasets/proc/laquila'
        rdir = '/gpfs/scratch/mdavid/datasets/proc/laquila'
        net = self.attr['network']
        sta = self.attr['station']
        chn = self.attr['channel']
        dat = str(UTCDateTime(self.attr['starttime']).date)
        dout = rdir + os.sep + net + os.sep + sta + os.sep + chn
        if (not os.path.exists(dout)):
            os.makedirs(dout, 0755)
        fout = dout + os.sep + dat + '.hdf'
        return fout

########################################################################
chain = []
chain.append( (PEGapCorr, {'gapcorr': 0 , 'tlength': 86400} ) )
chain.append(  PEMeanSub )
chain.append(  PEDetrend )
chain.append( (PEClip,  {'factor': 3} ) )
chain.append( (PEDecim, {'freqOut': 5.0} ) )
chain.append( (PEWhite, { 'flo' : 0.05, 'fhi' : 1.0 }) )

controlParameters = { 'runId' : '12345', 'username' : 'amyrosa',  'outputdest' : "./" }
composite = createProcessingComposite(chain, controlParameters=controlParameters)
findFiles = FindFiles()
producer = StreamProducer()
consumer = StreamConsumer()

graph = WorkflowGraph()
graph.connect(findFiles, FindFiles.OUTPUT_NAME, producer, StreamProducer.INPUT_NAME)
graph.connect(producer, StreamProducer.OUTPUT_NAME, composite, INPUT_NAME)
graph.connect(composite, OUTPUT_NAME, consumer, 'input')
