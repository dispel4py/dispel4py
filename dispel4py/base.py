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
Base PEs implementing typical processing patterns.
'''

from dispel4py.core import GenericPE, NAME


class BasePE(GenericPE):
    '''
    A basic implementation of a :py:class:`~dispel4py.core.GenericPE` that
    allows to easily extend the GenericPE with named inputs and outputs.
    '''
    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'

    def __init__(self, inputs=[], outputs=[], numInputs=0, numOutputs=0):
        '''
        :param inputs: a list of input names (optional)
        :param outputs: a list of output names (optional)
        :param numInputs: number of inputs; the inputs are generated as
        'input0' to 'input`n`' where `n` is the number of inputs (optional)
        :param numInputs: number of outputs; the outputs are generated as
        'output0' to 'output`n`' where `n` is the number of outputs (optional)
        '''
        GenericPE.__init__(self)

        for i in range(numInputs):
            name = '%s%s' % (BasePE.INPUT_NAME, i)
            self.inputconnections[name] = {NAME: name}
        for i in range(numOutputs):
            name = '%s%s' % (BasePE.OUTPUT_NAME, i)
            self.outputconnections[name] = {NAME: name}
        for name in inputs:
            self.inputconnections[name] = {NAME: name}
        for name in outputs:
            self.outputconnections[name] = {NAME: name}


class IterativePE(BasePE):
    '''
    An iterative PE that has one input and one output stream.
    When executed, this PE produces one output data block for each input block.
    Subclasses are expected to override
    :py:func:`~dispel4py.base.IterativePE._process` to implement processing.
    '''
    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'

    def __init__(self):
        BasePE.__init__(self)
        self._add_input(IterativePE.INPUT_NAME)
        self._add_output(IterativePE.OUTPUT_NAME)

    def process(self, inputs):
        '''
        Calls the implementation of
        :py:func:`~dispel4py.base.ConsumerPE._process` of the subclass with
        the data read from the input stream and writes the return value to the
        output stream.
        '''
        data = inputs[IterativePE.INPUT_NAME]
        result = self._process(data)
        if result is not None:
            return {self.OUTPUT_NAME: result}

    def _process(self, data):
        '''
        Processes a data block.
        :param data: the input data
        :returns: an output data block or None
        '''
        return None


class ProducerPE(BasePE):
    '''
    A PE that has no input and one output named "output".
    Subclasses are expected to override
    :py:func:`~dispel4py.base.ProducerPE._process` to implement processing.
    '''
    OUTPUT_NAME = 'output'

    def __init__(self):
        BasePE.__init__(self)
        self._add_output(ProducerPE.OUTPUT_NAME)

    def process(self, inputs):
        '''
        Calls the implementation of
        :py:func:`~dispel4py.base.ProducerPE._process` of the subclass.
        '''
        result = self._process()
        if result is not None:
            return {self.OUTPUT_NAME: result}

    def _process(self):
        return None


class ConsumerPE(BasePE):
    '''
    A PE that has one input named "input" and no outputs.
    Subclasses are expected to override
    :py:func:`~dispel4py.base.ConsumerPE._process` to implement processing.
    '''
    INPUT_NAME = 'input'

    def __init__(self):
        BasePE.__init__(self)
        self._add_input(ConsumerPE.INPUT_NAME)

    def process(self, inputs):
        '''
        Calls the implementation of
        :py:func:`~dispel4py.base.ConsumerPE._process` of the subclass with
        the data read from the input stream.
        '''
        data = inputs[ConsumerPE.INPUT_NAME]
        self._process(data)


class SimpleFunctionPE(IterativePE):
    INPUT_NAME = IterativePE.INPUT_NAME
    OUTPUT_NAME = IterativePE.OUTPUT_NAME

    def __init__(self, compute_fn=None, params={}):
        IterativePE.__init__(self)
        if compute_fn:
            self.name = 'PE_%s' % compute_fn.__name__
        self.compute_fn = compute_fn
        self.params = params

    def _process(self, data):
        return self.compute_fn(data, **self.params)


from dispel4py.workflow_graph import WorkflowGraph


def create_iterative_chain(functions,
                           FunctionPE_class=SimpleFunctionPE,
                           name_prefix='PE_',
                           name_suffix=''):

    '''
    Creates a composite PE wrapping a pipeline that processes obspy streams.
    :param chain: list of functions that process data iteratively. The function
    accepts one input parameter, data, and returns an output data block
    (or None).
    :param requestId: id of the request that the stream is associated with
    :param controlParameters: environment parameters for the processing
    elements
    :rtype: dictionary inputs and outputs of the composite PE that was created
    '''

    prev = None
    first = None
    graph = WorkflowGraph()

    for fn_desc in functions:
        try:
            fn = fn_desc[0]
            params = fn_desc[1]
        except TypeError:
            fn = fn_desc
            params = {}

        # print 'adding %s to chain' % fn.__name__
        pe = FunctionPE_class()
        pe.compute_fn = fn
        pe.params = params
        pe.name = name_prefix + fn.__name__ + name_suffix

        if prev:
            graph.connect(prev, IterativePE.OUTPUT_NAME,
                          pe, IterativePE.INPUT_NAME)
        else:
            first = pe
        prev = pe

    # Map inputs and outputs of the wrapper to the nodes in the subgraph
    graph.inputmappings = {'input': (first, IterativePE.INPUT_NAME)}
    graph.outputmappings = {'output': (prev, IterativePE.OUTPUT_NAME)}

    return graph


class CompositePE(WorkflowGraph):
    '''
    Super class for composite PEs. When extending or instantiating this PE
    a function may be provided to populate the graph. For example::

        def create_graph(graph):
            prod = Producer()
            cons = Consumer()
            graph.connect(prod, 'output', cons, 'input')
            graph._map_output('output', cons, 'output')


        class MyCompositePE(CompositePE):
            def __init__(self):
                CompositePE.__init__(self, create_graph)

    This composite PE can be created with the usual command::

        comp = MyCompositeTestPE()

    It is also possible to pass parameters to the composite PE when
    instantiating::

        def create_graph(limit):
            prod = Producer(limit)
            cons = Consumer()
            graph.connect(prod, 'output', cons, 'input')
            graph._map_output('output', cons, 'output')

        class ParameterisedCompositePE(CompositePE):
            def __init__(self, limit)
                CompositePE.__init__(self, create_graph, limit)
    '''
    def __init__(self, create_graph=None, params=None):
        '''
        Instantiate and populate the graph, if the function provided.
        Otherwise, the graph must be populated explicitly by the subclass or
        after instantiating the object.
        '''
        WorkflowGraph.__init__(self)
        self.inputmappings = {}
        self.outputmappings = {}
        if create_graph:
            if params is not None:
                create_graph(self, None)
            else:
                create_graph(self)

    def _map_input(self, input_name, internal_pe, internal_input):
        '''
        Map the composite PE input to the named input of a PE that is contained
        in the graph.
        '''
        self.inputmappings[input_name] = (internal_pe, internal_input)

    def _map_output(self, output_name, internal_pe, internal_output):
        '''
        Map the composite PE output to the named output of a PE that is
        contained in the graph.
        '''
        self.outputmappings[output_name] = (internal_pe, internal_output)
