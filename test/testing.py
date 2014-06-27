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

from numpy import random
from dispel4py.GenericPE import GenericPE, NAME, GROUPING

class RandomFilter(GenericPE):
    input_name = 'input'
    output_name = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections['input'] = { NAME : 'input' }
        out1 = {}
        out1[NAME] = "output"
        self.outputconnections["output"] = out1
    
    def process(self, inputs):
        if random.choice([True, False]):
            return { 'output' : inputs['input'] }
            # self.write('output', inputs['input'] )
        return None
            
class WordCounter(GenericPE):
    input_name = 'input'
    output_name = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections['input'] = { NAME : 'input', GROUPING : [0] }
        out1 = {}
        out1[NAME] = "output"
        self.outputconnections["output"] = out1
        self.mywords = {}
    
    def process(self, inputs):
        word = inputs['input'][0]
        try:
            self.mywords[word] += 1
        except KeyError:
            self.mywords[word] = 1
        return { 'output' : [word, self.mywords[word]]}

class RandomWordProducer(GenericPE):
    words = ["VERCE", "Earthquake", "Computing", "Seismology", "Modelling", "Analysis", "Infrastructure"]
    def __init__(self):
        GenericPE.__init__(self)
        out1 = {}
        out1[NAME] = "output"
        self.outputconnections["output"] = out1
    def process(self, inputs=None):
        word = random.choice(RandomWordProducer.words)
        outputs = { 'output' : [word] }
        return outputs
        
