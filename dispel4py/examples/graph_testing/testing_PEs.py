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
from dispel4py.base import IterativePE, ProducerPE, ConsumerPE
import random
import time
from collections import defaultdict


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

    def _process(self, inputs):
        for i in range(self.numIterations):
            self.write('output', [self.counter*i+i])
        self.counter += 1


class IntegerProducer(ProducerPE):

    def __init__(self, start, limit):
        ProducerPE.__init__(self)
        self.start = start
        self.limit = limit

    def _process(self, inputs):
        for i in range(self.start, self.limit):
            self.write('output', i)


class TestOneInOneOut(GenericPE):
    '''
    This PE outputs the input data.
    '''

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')

    def setInputTypes(self, inputtypes):
        self.inputconnections['input']['type'] = inputtypes['input']
        self.outputconnections['output']['type'] = inputtypes['input']

    def process(self, inputs):
        # self.log('Processing inputs %s' % inputs)
        return {'output': inputs['input']}


class TestIterative(IterativePE):
    '''
    This PE outputs the input data.
    '''

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, data):
        return data


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


class PrintDataConsumer(ConsumerPE):

    def __init__(self):
        ConsumerPE.__init__(self)

    def _process(self, data):
        print(data)


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
        self.mywords = defaultdict(int)

    def _process(self, inputs):
        word = inputs['input'][0]
        self.mywords[word] += 1
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
