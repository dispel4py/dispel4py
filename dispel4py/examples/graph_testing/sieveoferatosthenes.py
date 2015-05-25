# Copyright (c) The University of Edinburgh 2014-2015
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


from dispel4py.core import GenericPE
from dispel4py.base import IterativePE


class MyFirstPE(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('prime')
        self._add_output('output')
        self.divisor = None

    def _process(self, inputs):
        number = inputs['input']
        if not self.divisor:
            self.divisor = number
            return {'prime': number}
        if not number % self.divisor == 0:
            return {'output': number}


from dispel4py.base import ProducerPE


class NumberProducer(ProducerPE):

    def __init__(self, limit):
        ProducerPE.__init__(self)
        self.limit = limit

    def _process(self):
        for i in xrange(2, self.limit):
            self.write(ProducerPE.OUTPUT_NAME, i)


class PrimeCollector(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, data):
        return data


from dispel4py.workflow_graph import WorkflowGraph


graph = WorkflowGraph()
producer = NumberProducer(1000)
primes = PrimeCollector()
prev = producer
for i in range(2, 200):
    divide = MyFirstPE()
    graph.connect(prev, 'output', divide, 'input')
    prev = divide
    graph.connect(divide, 'prime', primes, 'input')
