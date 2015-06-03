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

from dispel4py.core import GenericPE
import random
import time


class TestProducer(GenericPE):
    '''
    This PE produces a range of numbers
    '''

    def __init__(self, numOutputs=1):
        GenericPE.__init__(self)
        if numOutputs == 1:
            self._add_output('output', tuple_type=['number'])
        else:
            for i in range(numOutputs):
                self._add_output('output%s' % i, tuple_type=['number'])
        self.counter = 0
        self.outputnames = list(self.outputconnections.keys())

    def _process(self, inputs):
        self.counter += 1
        result = {}
        for output in self.outputnames:
            result[output] = self.counter
        # self.log("Writing out %s" % result)
        return result


class NumberProducer(GenericPE):

    def __init__(self, numIterations=1):
        GenericPE.__init__(self)
        self._add_output('output', tuple_type=['number'])
        self.counter = 0
        self.numIterations = numIterations

    def process(self, inputs):
        for i in range(self.numIterations):
            self.write('output', [self.counter*i+i])
        self.counter += 1


class TestOneInOneOut(GenericPE):
    '''
    This PE outputs the input data.
    '''

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output', tuple_type=['number'])

    def process(self, inputs):
        # self.log('Processing inputs %s' % inputs)
        return {'output': inputs['input']}


class TestDelayOneInOneOut(GenericPE):
    '''
    This PE outputs the input data.
    '''

    def __init__(self, delay=1):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output', tuple_type=['number'])
        self.delay = delay

    def process(self, inputs):
        # self.log('Processing inputs %s' % inputs)
        time.sleep(self.delay)
        return {'output': inputs['input']}


class TestOneInOneOutWriter(GenericPE):
    '''
    This PE copies the input to an output, but it uses the write method.
    Remember that the write function allows to produce more than one output
    block within one processing step.
    '''

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output', tuple_type=['number'])

    def process(self, inputs):
        self.write('output', inputs['input'])


class TestTwoInOneOut(GenericPE):
    '''
    This PE takes two inputs and it merges the data into one output string.
    '''

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input0')
        self._add_input('input1')
        self._add_output('output', tuple_type=['result'])

    def process(self, inputs):
        # print '%s: inputs %s' % (self.id, inputs)
        result = ''
        for inp in self.inputconnections:
            if inp in inputs:
                result += '%s' % (inputs[inp])
        if result:
            # print '%s: result %s' % (self.id, result)
            return {'output': result}


class TestMultiProducer(GenericPE):

    def __init__(self, num_output=10):
        GenericPE.__init__(self)
        self._add_output('output')
        self.num_output = num_output

    def _process(self, inputs):
        for i in range(self.num_output):
            self.write('output', i)


class RandomFilter(GenericPE):
    '''
    This PE randomly filters the input.
    '''
    input_name = 'input'
    output_name = 'output'

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output', tuple_type=['word'])

    def process(self, inputs):
        if random.choice([True, False]):
            return {'output': inputs['input']}
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
        self._add_input('input', grouping=[0])
        self._add_output('output', tuple_type=['word', 'count'])
        self.mywords = {}

    def _process(self, inputs):
        word = inputs['input'][0]
        try:
            self.mywords[word] += 1
        except KeyError:
            self.mywords[word] = 1
        return {'output': [word, self.mywords[word]]}


class RandomWordProducer(GenericPE):
    '''
    This PE produces a random word as an output.
    '''
    words = ["dispel4py", "computing", "mpi", "processing",
             "simple", "analysis", "data"]

    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output', tuple_type=['word'])

    def process(self, inputs=None):
        word = random.choice(RandomWordProducer.words)
        outputs = {'output': [word]}
        return outputs


class ProvenanceLogger(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('metadata')

    def process(self, inputs):
        try:
            metadata = inputs['metadata']
            self.log('Logging metadata: %s' % str(metadata)[:300])
        except:
            import traceback
            self.log(traceback.format_exc())
