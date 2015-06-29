from nose import tools

import argparse

from dispel4py.new import processor as p
from dispel4py.workflow_graph import WorkflowGraph, WorkflowNode
from dispel4py.examples.graph_testing.testing_PEs \
    import TestProducer, TestTwoInOneOut


def test_roots():
    graph = WorkflowGraph()
    prod1 = TestProducer()
    prod2 = TestProducer()
    cons = TestTwoInOneOut()
    graph.connect(prod1, 'output', cons, 'input1')
    graph.connect(prod2, 'output', cons, 'input2')
    roots = set()
    non_roots = set()
    for node in graph.graph.nodes():
        if node.getContainedObject() == cons:
            non_roots.add(node)
        else:
            roots.add(node)
    for r in roots:
        tools.ok_(p._is_root(r, graph))
    for n in non_roots:
        tools.eq_(False, p._is_root(n, graph))


@tools.raises(ValueError)
def test_input_invalid():
    args = argparse.Namespace
    args.data = '{not valid'
    args.file = None
    args.iter = 1
    p.create_inputs(args, None)


def test_input_json():
    args = argparse.Namespace
    args.data = '{ "TestProducer": 20}'
    args.file = None
    args.iter = 1
    graph = WorkflowGraph()
    prod = TestProducer()
    graph.add(prod)
    inputs = p.create_inputs(args, graph)
    tools.eq_(inputs[prod.id], 20)


def test_input_iter():
    args = argparse.Namespace
    args.file = None
    args.data = None
    args.iter = 20
    graph = WorkflowGraph()
    prod = TestProducer()
    graph.add(prod)
    inputs = p.create_inputs(args, graph)
    tools.eq_(inputs[prod.id], 20)


def test_input_iter_one():
    args = argparse.Namespace
    args.file = None
    args.data = None
    args.iter = 1
    graph = WorkflowGraph()
    prod = TestProducer()
    graph.add(prod)
    inputs = p.create_inputs(args, graph)
    tools.eq_(inputs[prod.id], 1)


def test_input_file():
    args = argparse.Namespace
    import tempfile
    namedfile = tempfile.NamedTemporaryFile()
    with namedfile as temp:
        data = '{ "TestProducer": 20}'
        try:
            temp.write(data)
        except:
            temp.write(bytes(data, 'UTF-8'))
        temp.flush()
        temp.seek(0)
        args.file = namedfile.name
        args.data = None
        args.iter = 1
        graph = WorkflowGraph()
        prod = TestProducer()
        graph.add(prod)
        inputs = p.create_inputs(args, graph)
        tools.eq_(inputs[prod.id], 20)


@tools.raises(ValueError)
def test_invalid_input_file():
    args = argparse.Namespace
    import tempfile
    namedfile = tempfile.NamedTemporaryFile()
    with namedfile as temp:
        data = '{ bla'
        try:
            temp.write(data)
        except:
            temp.write(bytes(data, 'UTF-8'))
        temp.flush()
        temp.seek(0)
        args.file = namedfile.name
        args.data = None
        args.iter = 1
        p.create_inputs(args, None)


@tools.raises(ValueError)
def test_input_file_not_found():
    args = argparse.Namespace
    args.file = '/doesnotexist'
    args.data = None
    args.iter = 1
    p.create_inputs(args, None)


def test_load_graph():
    args = argparse.Namespace
    args.file = None
    args.data = None
    args.iter = 1
    args.attr = None
    args.module = 'dispel4py.examples.graph_testing.pipeline_test'
    # reset the node counter
    WorkflowNode.node_counter = 0
    graph, inputs = p.load_graph_and_inputs(args)
    tools.ok_(graph)
    tools.eq_({'TestProducer0': 1}, inputs)


def test_group_by():
    destinations = list(range(5))
    input_name = 'input'
    groupby = [0]
    comm = p.GroupByCommunication(destinations, input_name, groupby)
    tools.eq_(comm.getDestination({'input': [1, 'b']}),
              comm.getDestination({'input': [1, 1, 2]}))
    tools.eq_(comm.getDestination({'input': [{'a': 1}, 'b']}),
              comm.getDestination({'input': [{'a': 1}, 2]}))
    tools.eq_(comm.getDestination({'input': [set(range(7)), 'b']}),
              comm.getDestination({'input': [set(range(7)), '_', 10]}))
    tools.eq_(comm.getDestination({'input': [range(1), 'x', 'y']}),
              comm.getDestination({'input': [range(1), 1, 2]}))
    tools.eq_(comm.getDestination({'input': [None, 'x', 'y']}),
              comm.getDestination({'input': [None, 1, 2]}))


def test_all_to_one():
    destinations = list(range(5))
    comm = p.AllToOneCommunication(destinations)
    tools.eq_([0], comm.getDestination(None))
    tools.eq_([0], comm.getDestination({'input': [1, 2, 3]}))


def test_one_to_all():
    destinations = list(range(5))
    comm = p.OneToAllCommunication(destinations)
    tools.eq_(destinations, comm.getDestination(None))
    tools.eq_(destinations, comm.getDestination({'input': [1, 2, 3]}))
