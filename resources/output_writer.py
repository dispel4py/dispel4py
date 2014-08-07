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

class OutputWriter(object):
    
    def __init__(self, scriptname, streamname):
        self.scriptname = scriptname
        self.streamname = streamname
        
    def write(self, output):
        result = output if isinstance(output, list) else [output]
        try:
            storm.emit(result, stream=self.streamname)
            storm.log("Dispel4Py ------> Emitted to stream %s." % (self.scriptname, self.streamname))
        except TypeError:
            # encode manually
            encoded = encode_types(result)
            storm.emit(encoded, stream=self.streamname)
            storm.log("Dispel4Py ------> Emitted to stream %s." % (self.scriptname, self.streamname))
        
import json
import numpy
import base64

def encode_types(obj):
    # storm.log('encoding %s' % str(obj))
    new_obj = obj
    if isinstance(obj, (tuple, list)):
        new_obj = []
        for i in obj:
            new_obj.append(encode_types(i))
    elif isinstance(obj, set):
        new_obj = set()
        for i in obj:
            new_obj.add(encode_types(i))
    elif isinstance(obj, dict):
        new_obj = dict()
        for k, v in obj.iteritems():
            new_obj[k]=encode_types(v)
    elif isinstance(obj, numpy.ndarray):
        new_obj = { 
            '__dispel4py.type__' : 'numpy.ndarray', 
            'dtype' : obj.dtype.name, 
            'data' : base64.b64encode(obj)
        }
    return new_obj
    
def decode_types(obj):
    new_obj = obj
    if isinstance(obj, (tuple, list)):
        new_obj = type(obj)()
        for i in obj:
            new_obj.append(decode_types(i))
    elif isinstance(obj, set):
        new_obj = set()
        for i in obj:
            new_obj.add(decode_types(i))
    elif isinstance(obj, dict):
        try:
            if obj['__dispel4py.type__'] == 'numpy.ndarray':
                r = base64.decodestring(obj['data'])
                return numpy.frombuffer(r, dtype=obj['dtype'])
        except KeyError:
            pass
        # if it's just a normal dictionary then decode recursively
        new_obj = dict()
        for k, v in obj.iteritems():
            new_obj[k]=decode_types(v)
    return new_obj
