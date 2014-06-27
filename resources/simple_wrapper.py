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

import storm
import traceback
from importlib import import_module
import pickle
from output_writer import OutputWriter

class SimpleWrapper(storm.Bolt):

    def initialize(self, conf, context):
        try:        
            self.modname = conf["dispel4py.module"]
            self.scriptname = conf["dispel4py.script"]
            self.inputmapping = conf['dispel4py.inputmapping']
            scriptconfig = pickle.loads(str(conf['dispel4py.config'])) if 'dispel4py.config' in conf else {}

            storm.log("Dispel4Py ------> loading script %s" % self.scriptname)
            mod = import_module(self.modname)
            self.script = getattr(mod, self.scriptname)()
            for key, value in scriptconfig.iteritems():
                storm.log("Dispel4Py ------> %s: setting attribute %s" % (self.scriptname, key))
                setattr(self.script, key, value)
            storm.log("Dispel4Py ------> loaded script %s" % self.scriptname)
                
            # attach an output writer to each output connection
            for outputname, output in self.script.outputconnections.iteritems():
                output['writer'] = OutputWriter(self.scriptname, outputname)
                
            self.boltId = "%s (%s)" % (self.scriptname, self.script.boltId) if hasattr(self.script, 'boltId') else self.scriptname
            # pre-processing if required
            self.script.preprocess()
            storm.log("Dispel4Py ------> %s: preprocess() completed." % (self.boltId,))
        except:
            storm.log("Dispel4Py ------> %s: %s" % (self.scriptname, traceback.format_exc(),))
            raise
            
    def process(self,tup):
        # storm.log("Dispel4Py ------> %s: Received block." % (self.boltId, ))
        try:
            inputname = self.inputmapping[tup.component][tup.stream]
            storm.log("Dispel4Py ------> %s: Received block at input '%s'" % (self.script.id, inputname, ))
            outputs = self.script.process( { inputname : tup.values })
            # storm.log("Dispel4Py ------> %s: Processing complete." % self.scriptname)
            
            if outputs is None:
                return
            for streamname, output in outputs.iteritems():
                tuple = output if isinstance(output, list) else [output]
                storm.emit(tuple, stream=streamname)
                storm.log("Dispel4Py ------> %s: Emitted output: %s" % (self.script.id, str(tuple)[:400]))
                
            
        except:
            storm.log("Dispel4Py ------> %s: %s" % (self.boltId, traceback.format_exc(), ))

if __name__ == "__main__":
    SimpleWrapper().run()
