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

from dispel4py.workflow_graph import WorkflowGraph

from dispel4py.registry import core
# reg = core.initRegistry(username='admin', password='admin', url='https://escience8.inf.ed.ac.uk:443')
reg = core.initRegistry(username='iraklis', password='iraklis')

import Local

from pes.RandomWordProducer import RandomWordProducer
from pes.RandomFilter import RandomFilter

from fns.reg_random_int import reg_random_int

# Direct implementation import
from impls.RandomIntImpl2 import reg_random_int as rand2

# print reg_random_int.__doc__
print '> ' + str(reg_random_int(10,100))
print '>>' + str(rand2(10,100))

words = RandomWordProducer()
filter = RandomFilter()

graph = WorkflowGraph()
graph.connect(words, 'output', filter, 'input')

