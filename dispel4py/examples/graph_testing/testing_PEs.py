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

# Test PEs
'''
Example PEs for test workflows, implementing various patterns.
'''

from dispel4py.core import GenericPE, NAME, TYPE, GROUPING
from numpy import random

class TestProducer(GenericPE):
    '''
    This PE produces a range of numbers
    '''
    def __init__(self, numOutputs=1):
        GenericPE.__init__(self)
        if numOutputs == 1:
            self.outputconnections = { 'output' : { NAME : 'output', TYPE: ['number'] } }
        else:
            for i in range(numOutputs):
                self.outputconnections['output%s' % i] = { NAME : 'output%s' % i, TYPE: ['number'] } 
        self.counter = 0
        self.outputnames = list(self.outputconnections.keys())
    def process(self, inputs):
        self.counter += 1
        result = {}
        for output in self.outputnames:
            result[output] = self.counter
        return result
        
class NumberProducer(GenericPE):
    def __init__(self, numIterations=1):
        GenericPE.__init__(self)
        self.outputconnections = { 'output' : { NAME : 'output', TYPE: ['number'] } }
        self.counter = 0
        self.numIterations = numIterations
    def process(self, inputs):
        self.counter += 1
        for i in range(self.numIterations):
            self.write('output', '%s:%s' % (self.counter, i))

class TestOneInOneOut(GenericPE):
    '''
    This PE copies the input to an output. 
    '''
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
        self.outputconnections = { 'output' : { NAME : 'output', TYPE: ['number'] } }
    def process(self, inputs):
        # print '%s: Processing inputs %s' % (self, inputs)
        return { 'output' : inputs['input'] }

class TestOneInOneOutWriter(GenericPE):
    '''
    This PE copies the input to an output, but it uses the write method.
    Remeber that the write function allows to produce more than one output block within one processing step.  
    '''
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input' : { NAME : 'input' } }
        self.outputconnections = { 'output' : { NAME : 'output', TYPE: ['number'] } }
    def process(self, inputs):
        self.write('output', inputs['input'])

class TestTwoInOneOut(GenericPE):
    '''
    This PE takes two inputs and it merges into one oputput.  
    '''
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'input0' : { NAME : 'input0' }, 'input1' : { NAME : 'input1' } }
        self.outputconnections = { 'output' : { NAME : 'output', TYPE: ['result'] } }
    def process(self, inputs):
        # print '%s: inputs %s' % (self.id, inputs)
        result = ''
        for inp in self.inputconnections:
            if inp in inputs:
                result += '%s' % (inputs[inp])
        if result:
            # print '%s: result %s' % (self.id, result)
            return { 'output' : result }


class RandomFilter(GenericPE):
    '''
    This PE randomly filters (in case "false")  the input, and produce the same output (in case "true").   
    '''
    input_name = 'input'
    output_name = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections['input'] = { NAME : 'input' }
        out1 = {}
        out1[NAME] = "output"
        out1[TYPE] = ['word']
        self.outputconnections["output"] = out1
    
    def process(self, inputs):
        if random.choice([True, False]):
            return { 'output' : inputs['input'] }
            # self.write('output', inputs['input'] )
        return None
            
class WordCounter(GenericPE):
    '''
    This PE counts the number of times (counter) that it receives each word.
    And it produces as an output: the same word (the input) and its counter. 
    '''
    input_name = 'input'
    output_name = 'output'
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections['input'] = { NAME : 'input', GROUPING : [0] }
        out1 = {}
        out1[NAME] = "output"
        out1[TYPE] = ['word', 'count']
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
    '''
    This PE produces a random word as an output. 

    '''
    words = ["Dispel4Py", "Earthquake", "Computing", "Seismology", "Modelling", "Analysis", "Infrastructure"]
    def __init__(self):
        GenericPE.__init__(self)
        out1 = {}
        out1[NAME] = "output"
        out1[TYPE] = ['word']
        self.outputconnections["output"] = out1
    def process(self, inputs=None):
        word = random.choice(RandomWordProducer.words)
        outputs = { 'output' : [word] }
        return outputs

import traceback

class ProvenanceLogger(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self.inputconnections = { 'metadata' : { NAME : 'metadata' } }
    def process(self, inputs):
        try:
            metadata = inputs['metadata']
            self.log('Logging metadata: %s' % str(metadata)[:300])
        except:
            self.log(traceback.format_exc())
