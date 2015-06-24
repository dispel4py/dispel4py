# Copyright (c) The University of Edinburgh 2015
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

'''
Tests for simple sequential processing engine.

Using nose (https://nose.readthedocs.org/en/latest/) run as follows::

    $ nosetests dispel4py/test/base_test.py
'''

from dispel4py.base import BasePE, IterativePE, ProducerPE, ConsumerPE, \
    SimpleFunctionPE

from nose import tools


def testBaseNumInputs():
    base = BasePE(num_inputs=3, num_outputs=2)
    tools.eq_(len(base.outputconnections), 2)
    tools.eq_(len(base.inputconnections), 3)


def testBaseInputs():
    base = BasePE(inputs=['a', 'b'], outputs=['x', 'y'])
    tools.eq_(set(base.inputconnections.keys()), set(['a', 'b']))
    tools.eq_(set(base.outputconnections.keys()), set(['x', 'y']))


def testIterative():
    it = IterativePE()
    tools.ok_(it._process(1) is None)


class OneProducer(ProducerPE):
    def __init__(self):
        ProducerPE.__init__(self)

    def _process(self, data):
        return 1


def testProducer():
    prod = OneProducer()
    tools.eq_({'output': 1}, prod.process(None))


def testConsumer():
    cons = ConsumerPE()
    tools.ok_(cons.process({'input': 1}) is None)


def testSimpleFunction():
    def add(a, b):
        return a+b
    simp = SimpleFunctionPE(add, {'b': 2})
    tools.eq_({'output': 3}, simp.process({'input': 1}))
