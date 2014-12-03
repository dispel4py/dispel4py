# Copyright (c) The University of Edinburgh 2014
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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