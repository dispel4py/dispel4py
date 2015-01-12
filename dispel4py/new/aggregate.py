from dispel4py.workflow_graph import WorkflowGraph


class AggregatePE(GenericPE):
    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'
    def __init__(self, index=0):
        GenericPE.__init__(self)
        self.index = index
        self.value = 0
    def _postprocess(self):
        self.write(self.value)


class CountPE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self)
    def _process(self, inputs):
        self.count += 1


class MaxPE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self)
    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME][self.index]
        if (v > self.value):
            self.value = v


class MinPE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self)
        self.value = None
    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME][self.index]
        if (self.value = None or v < self.value):
            self.value = v


class SumPE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self)
    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME][self.index]
        self.value += v


class AveragePE(AggregatePE):
    def __init__(self):
        AggregatePE.__init__(self)
        self.sum = 0;
        self.count = 0;
    def _process(self, inputs):
        v = inputs[AggregatePE.INPUT_NAME][self.index]
        self.sum += v
        self.count += 1
    def _postprocess(self):
        self.write((self.sum/float(self.count), self.sum, self.count))


def parallel_aggregate(instPE, reducePE):
    composite = WorkflowGraph()
    reducePE.inputconnections[AggregatePE.INPUT_NAME]['grouping'] = 'global'
    composite.connect(instCount, AggregatePE.OUTPUT_NAME, sumCount, AggregatePE.INPUT_NAME)
    composite.inputmappings = { 'input' : (instCount, AggregatePE.INPUT_NAME) }
    composite.outputmappings = { 'output' : (redCount, AggregatePE.OUTPUT_NAME) }
    return composite


def count():
    '''
    Creates a counter composite PE that is parallelisable using a map-reduce pattern.
    The first part of the composite PE is a counter that counts all the inputs,
    the second part sums up the counts of the counter instances.
    '''
    return parallel_aggregate(CountPE(), SumPE())


def sum():
    '''
    Creates a SUM composite PE that can be parallelised using a map-reduce pattern.
    '''
    return parallel_aggregate(SumPE(), SumPE())


def min():
    return parallel_aggregate(MinPE(), MinPE())


def max():
    return parallel_aggregate(MaxPE(), MaxPE())


def avg():
    return parallel_aggregate(AveragePE(), AveragePE())