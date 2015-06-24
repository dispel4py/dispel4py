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

'''
Tests for multiprocessing mapping.

Using nose (https://nose.readthedocs.org/en/latest/) run as follows::

    $ nosetests dispel4py/new/multi_process_test.py
    ...
    ----------------------------------------------------------------------
    Ran 3 tests in 0.042s

    OK
'''
import argparse
from collections import Counter, defaultdict

from nose import tools

from dispel4py.examples.graph_testing.testing_PEs\
    import TestProducer, TestOneInOneOut, TestTwoInOneOut, NumberProducer
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.new.multi_process import process, STATUS_TERMINATED


args = argparse.Namespace
args.num = 5
args.simple = False


def testPipeline():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    args = argparse.Namespace
    args.num = 4
    args.simple = False
    args.results = True
    result_queue = process(graph, inputs={prod: 5}, args=args)
    results = []
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_(cons2.id, name)
        tools.eq_('output', output)
        results.append(data)
        item = result_queue.get()
    tools.eq_(list(range(1, 6)), results)


def testSquare():
    graph = WorkflowGraph()
    prod = TestProducer(2)
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    last = TestTwoInOneOut()
    graph.connect(prod, 'output0', cons1, 'input')
    graph.connect(prod, 'output1', cons2, 'input')
    graph.connect(cons1, 'output', last, 'input0')
    graph.connect(cons2, 'output', last, 'input1')
    args.num = 4
    args.results = True
    result_queue = process(graph, inputs={prod: 10}, args=args)
    results = []
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_(last.id, name)
        tools.eq_('output', output)
        results.append(data)
        item = result_queue.get()
    expected = {str(i): 2 for i in range(1, 11)}
    tools.eq_(expected, Counter(results))


def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    args.num = 3
    args.results = True
    result_queue = process(graph, inputs={prod: 5}, args=args)
    results = defaultdict(list)
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_('output', output)
        results[name].append(data)
        item = result_queue.get()
    tools.eq_(list(range(1, 6)), results[cons1.id])
    tools.eq_(list(range(1, 6)), results[cons2.id])


def testPipelineSimple():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    args = argparse.Namespace
    args.num = 4
    args.simple = True
    args.results = True
    result_queue = process(graph, inputs={prod: 5}, args=args)
    results = []
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_((cons2.id, 'output'), output)
        results.extend(data)
        item = result_queue.get()
    tools.eq_(Counter(range(1, 6)), Counter(results))


def testPipelineNotEnoughProcesses():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    cons3 = TestOneInOneOut()
    cons4 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons2, 'output', cons3, 'input')
    graph.connect(cons3, 'output', cons4, 'input')
    args = argparse.Namespace
    args.num = 4
    args.simple = False
    args.results = True
    result_queue = process(graph, inputs={prod: 10}, args=args)
    results = []
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_((cons4.id, 'output'), output)
        results.extend(data)
        item = result_queue.get()
    tools.eq_(Counter(range(1, 11)), Counter(results))


def testNotEnoughProcesses():
    prod = TestProducer()
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    args = argparse.Namespace
    args.num = 1
    args.simple = False
    args.results = True
    message = process(graph, inputs={prod: 5}, args=args)
    tools.ok_('Not enough processes' in message)


def testGroupings():
    prod = NumberProducer(10)
    cons1 = TestOneInOneOut()
    cons1.inputconnections['input']['type'] = [0]
    cons2 = TestOneInOneOut()
    cons1.inputconnections['input']['type'] = 'all'
    cons3 = TestOneInOneOut()
    cons1.inputconnections['input']['type'] = 'global'
    cons4 = TestOneInOneOut()
    graph = WorkflowGraph()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(cons1, 'output', cons2, 'input')
    graph.connect(cons2, 'output', cons3, 'input')
    graph.connect(cons3, 'output', cons4, 'input')
    args = argparse.Namespace
    args.num = 5
    args.simple = False
    args.results = True
    result_queue = process(graph, inputs={prod: 1}, args=args)
    results = []
    item = result_queue.get()
    while item != STATUS_TERMINATED:
        name, output, data = item
        tools.eq_(cons4.id, name)
        tools.eq_('output', output)
        results.extend(data)
        item = result_queue.get()
    tools.eq_(Counter(range(10)), Counter(results))


# mokey patch multiprocessing to enable  code coverage
# NOTE: doesnt work with pytest-xdist (actually execnet)
# https://bitbucket.org/ned/coveragepy/issue/117/
# enable-coverage-measurement-of-code-run-by
def coverage_multiprocessing_process():    # pragma: no cover
    try:
        import coverage as _coverage
        _coverage
    except:
        return

    from coverage.collector import Collector
    from coverage.control import coverage
    # detect if coverage was running in forked process
    if Collector._collectors:
        original = multiprocessing.Process._bootstrap

        class Process_WithCoverage(multiprocessing.Process):

            def _bootstrap(self):
                cov = coverage(data_suffix=True)
                cov.start()
                try:
                    return original(self)
                finally:
                    cov.stop()
                    cov.save()
        return Process_WithCoverage

import multiprocessing

ProcessCoverage = coverage_multiprocessing_process()
if ProcessCoverage:
    multiprocessing.Process = ProcessCoverage
