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

'''
This is a dispel4py graph that shows the group-by data pattern to count words.
'''

from dispel4py.examples.graph_testing import testing_PEs as t
from dispel4py.workflow_graph import WorkflowGraph


def testGrouping():
    '''
    Creates the test graph.
    '''
    words = t.RandomWordProducer()
    cons1 = t.TestOneInOneOut()
    cons2 = t.TestOneInOneOut()
    cons3 = t.TestOneInOneOut()
    count = t.WordCounter()
    graph = WorkflowGraph()
    graph.connect(words, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons2, 'output', cons3, 'input')
    graph.connect(cons3, 'output', count, 'input')

    graph.partitions = [[words], [cons1, cons2, cons3], [count]]
    return graph

graph = testGrouping()
