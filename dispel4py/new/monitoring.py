import time

class MonitoringWrapper(object):
    
    def __init__(self, baseObject):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.baseObject = baseObject


class ReadTimingWrapper(MonitoringWrapper):
    
    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.readtime = None
        self.readrate = []
    
    def _read(self):
        now = time.time()
        if self.readtime:
            self.readrate.append(now-self.readtime)
        self.readtime = now
        return self.baseObject._read()
        
    def _terminate(self):
        print "Average read rate : %s" % (sum(self.readrate)/float(len(self.readrate)))
        self.baseObject._terminate()
    
class ProcessTimingPE(MonitoringWrapper):
    
    def __init__(self, baseObject):
        MonitoringWrapper.__init__(self, baseObject)
        self.times = []
        
    def process(self, inputs):
        start = time.time()
        result = self.baseObject.process(inputs)
        self.times.append(time.time()-start)
        return result
    
    def _postprocess(self):
        self.log('Average processing time: %s' % (sum(self.times)/len(self.times)))