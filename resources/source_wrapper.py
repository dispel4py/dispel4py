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

class SourceWrapper(storm.Spout):

    def initialize(self, conf, context):
        try:        
            self.modname = conf["dispel4py.module"]
            self.scriptname = conf["dispel4py.script"]
            
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
                
            # pre-processing if required
            self.script.preprocess()
            storm.log("Dispel4Py ------> %s: preprocess() completed." % (self.scriptname,))
        except:
            storm.log("Dispel4Py ------> %s: %s" % (self.scriptname, traceback.format_exc(),))
            raise
        
    def nextTuple(self):
        try:
            input_tuple = None
            try:
                input_tuple = self.script._static_input.pop(0)
            except AttributeError:
                # there is no static input
                pass
            except IndexError:
                # static input is empty - no more processing
                return
            outputs = self.script.process(input_tuple)
            if outputs is None:
                return
            for streamname, output in outputs.iteritems():
                result = output if isinstance(output, list) else [output]
                storm.emit(result, stream=streamname)
                storm.log("Dispel4Py ------> %s: emitted tuple %s to stream %s" % (self.script.id, result, streamname))
        except:
            # logging the error but it should be passed to client somehow
            storm.log("Dispel4Py ------> %s: %s" % (self.scriptname, traceback.format_exc(), ))
        
        
if __name__ == "__main__":
    SourceWrapper().run()
