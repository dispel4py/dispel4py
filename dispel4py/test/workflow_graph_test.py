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

    $ nosetests dispel4py/test/workflow_graph_test.py
'''

from nose import tools

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.workflow_graph import draw
from dispel4py.examples.graph_testing.testing_PEs \
    import TestProducer, TestOneInOneOut
from dispel4py.base import create_iterative_chain


def test_types():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph.connect(prod, 'output', cons, 'input')
    graph.propagate_types()
    tools.eq_(prod.outputconnections['output']['type'],
              cons.inputconnections['input']['type'])


def test_dot_pipeline():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph.connect(prod, 'output', cons, 'input')
    draw(graph)


def test_dot_composite():

    def inc(a):
        return a+1

    def dec(a):
        return a-1

    graph = WorkflowGraph()
    prod = TestProducer()
    comp = create_iterative_chain([inc, dec])
    cons = TestOneInOneOut()
    graph.connect(prod, 'output', comp, 'input')
    graph.connect(comp, 'output', cons, 'input')
    graph.inputmappings = {'input': (prod, 'input')}
    root_prod = TestProducer()
    root_graph = WorkflowGraph()
    root_graph.connect(root_prod, 'output', graph, 'input')
    dot = draw(root_graph)
    tools.ok_('subgraph cluster_' in dot)
