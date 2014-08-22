from dispel4py.seismo.seismo import SeismoPE
from dispel4py.core import GenericPE, NAME

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
        stream=read(self.st)
        stream.merge() # fills gaps with 0, gap correction should be implemented as PE
        outputattr = dict(self.attr)
        outputattr['starttime'] = str(stream[0].stats.starttime)
        outputattr['endtime'] = str(stream[0].stats.endtime)
        outputattr['sampling_rate'] = stream[0].stats.sampling_rate
        outputattr['npts'] = stream[0].stats.npts
        outputattr['location'] = stream[0].stats.location
        outputattr['network'] = stream[0].stats.network
        outputattr['channel'] = stream[0].stats.channel
        outputattr['station'] = stream[0].stats.station

        self._timestamp['starttime'] = outputattr['starttime']
        self._timestamp['endtime'] = outputattr['endtime']
        self._location['network'] = outputattr['network']
        self._location['channel'] = outputattr['channel']
        self._location['station'] = outputattr['station']

        self.outputattr=[outputattr]
        self.outputstreams = [stream[0].data]

class DetrendPE(SeismoPE):
     def __init__(self):
         SeismoPE.__init__(self)
     def compute(self):
         return scipy.signal.detrend(self.st, type='linear')   
    