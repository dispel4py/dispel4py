import argparse

from dispel4py.examples.graph_testing.testing_PEs import TestProducer, TestOneInOneOut, TestTwoInOneOut
from dispel4py.workflow_graph import WorkflowGraph
from multi_process import process


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
    args.num = 5
    args.simple = False
    process(graph, inputs={ prod : [ {}, {}, {}  ] }, args=args )
    
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
    process(graph, inputs={ prod : [{}]}, args=args )

def testTee():
    graph = WorkflowGraph()
    prod = TestProducer()
    prev = prod
    cons1 = TestOneInOneOut()
    cons2 = TestOneInOneOut()
    graph.connect(prod, 'output', cons1, 'input')
    graph.connect(prod, 'output', cons2, 'input')
    args.num = 3
    process(graph, inputs={prod: [{}, {}, {}, {}, {}]}, args=args)

print '='*20 + 'PIPELINE' + '='*20 
testPipeline()
print '='*20 + 'SQUARE  ' + '='*20 
testSquare()
print '='*20 + 'TEE     ' + '='*20 
testTee()