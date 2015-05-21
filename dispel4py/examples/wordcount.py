from dispel4py.core import GenericPE
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
    graph = CompositePE()
    count = CountByGroup()
    merge = CountByGroup()
    merge._add_input('input', grouping='global')
    graph.connect(count, 'output', merge, 'input')
    graph._map_input('input', count, 'input')
    graph._map_output('output', merge, 'output')
    return graph


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