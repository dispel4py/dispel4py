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
Tests for aggregation processing elements.

Using nose (https://nose.readthedocs.org/en/latest/) run as follows::

    nosetests dispel4py/new/aggregate_test.py
    ....
    ----------------------------------------------------------------------
    Ran 4 tests in 0.023s

    OK
'''

from dispel4py.examples.graph_testing.testing_PEs import NumberProducer
from dispel4py.new.aggregate \
    import parallelSum, parallelCount, parallelMin, parallelMax, parallelAvg

from dispel4py.new import simple_process
from dispel4py.workflow_graph import WorkflowGraph

from nose import tools


def graph_sum():
    prod = NumberProducer(1000)
    prod.name = 'NumberProducer'
    s = parallelSum()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', s, 'input')
    return graph


def graph_avg():
    prod = NumberProducer(1000)
    a = parallelAvg()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', a, 'input')
    return graph


def graph_min_max():
    prod = NumberProducer(1000)
    mi = parallelMin()
    ma = parallelMax()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', mi, 'input')
    graph.connect(prod, 'output', ma, 'input')
    return graph


def graph_count():
    prod = NumberProducer(1000)
    c = parallelCount()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', c, 'input')
    return graph


def testSum():
    sum_wf = graph_sum()
    sum_wf.flatten()
    results = simple_process.process_and_return(
        sum_wf,
        inputs={'NumberProducer': [{}]})
    # there should be only one result
    tools.eq_(1, len(results))
    for key in results:
        tools.eq_({'output': [[499500]]}, results[key])


def testAvg():
    avg_wf = graph_avg()
    avg_wf.flatten()
    results = simple_process.process_and_return(
        avg_wf,
        inputs={'NumberProducer': [{}]})
    tools.eq_(1, len(results))
    for key in results:
        tools.eq_({'output': [[499.5, 1000, 499500]]}, results[key])


def testCount():
    count_wf = graph_count()
    count_wf.flatten()
    results = simple_process.process_and_return(
        count_wf,
        inputs={'NumberProducer': [{}]})
    print results
    tools.eq_(1, len(results))
    for key in results:
        tools.eq_({'output': [[1000]]}, results[key])


def testMinMax():
    min_max_wf = graph_min_max()
    min_max_wf.flatten()
    results = simple_process.process_and_return(
        min_max_wf,
        inputs={'NumberProducer': [{}]})
    print results
    tools.eq_(2, len(results))
    for key in results:
        if key.startswith('MinPE'):
            tools.eq_({'output': [[0]]}, results[key])
        else:
            tools.eq_({'output': [[999]]}, results[key])


sum_wf = graph_sum()
avg_wf = graph_avg()
min_wf = graph_min_max()
count_wf = graph_count()
