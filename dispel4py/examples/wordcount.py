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


from dispel4py.base import IterativePE, ConsumerPE, CompositePE

from collections import defaultdict


class SplitTextFile(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, filename):
        with open(filename) as f:
            for line in f:
                self.write('output', line.strip())


class WordCount(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)

    def _process(self, line):
        for w in line.split(' '):
            self.write('output', (w, 1))


class CountByGroup(IterativePE):

    def __init__(self):
        IterativePE.__init__(self)
        self.count = defaultdict(int)

    def _process(self, data):
        self.count[data[0]] += data[1]

    def _postprocess(self):
        for (word, count) in self.count.items():
            self.write('output', (word, count))


class Print(ConsumerPE):

    def __init__(self):
        ConsumerPE.__init__(self)

    def _process(self, data):
        self.log('%s: %s' % data)


def count_by_group():
    composite = CompositePE()
    count = CountByGroup()
    merge = CountByGroup()
    merge._add_input('input', grouping='global')
    composite.connect(count, 'output', merge, 'input')
    composite._map_input('input', count, 'input')
    composite._map_output('output', merge, 'output')
    return composite


from dispel4py.workflow_graph import WorkflowGraph

graph = WorkflowGraph()
textfile = SplitTextFile()
wordcount = WordCount()
# count = CountByGroup()
# merge = CountByGroup()
# merge._add_input('input', grouping='global')
count = count_by_group()
result = Print()

graph.connect(textfile, 'output', wordcount, 'input')
graph.connect(wordcount, 'output', count, 'input')
# graph.connect(count, 'output', merge, 'input')
# graph.connect(merge, 'output', result, 'input')
graph.connect(count, 'output', result, 'input')
