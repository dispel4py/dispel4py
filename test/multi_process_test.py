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

from nose import tools
from testing import TestProducer, TestOneInOneOut
from dispel4py.workflow_graph import WorkflowGraph
from dispel4py.multi_process import multiprocess

def testPipeline():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph.connect(prod, 'output', cons, 'input')
    multiprocess(graph, 2, [{}, {}, {}, {}, {}])
    
def testPipelineMulti():
    graph = WorkflowGraph()
    prod = TestProducer()
    cons = TestOneInOneOut()
    graph.connect(prod, 'output', cons, 'input')
    multiprocess(graph, 6, [{}, {}, {}, {}, {}])
