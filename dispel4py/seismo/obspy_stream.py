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

from dispel4py.seismo.seismo import SeismoPE
import traceback

INPUT_NAME = 'input'
OUTPUT_NAME = 'output'


class ObspyStreamPE(SeismoPE):
    '''
    A SeismoPE that calls a function to process an input stream.
    '''

    def __init__(self):
        SeismoPE.__init__(self)

    def setCompute(self, compute_fn, params={}):
        '''
        Define the compute function that this PE uses for processing input
        streams, and any input parameters for the function.
        The function must have at least one input, an obspy stream, and can
        accept more input parameters that must be provided before the PE is
        executed.
        '''
        self.compute_fn = compute_fn, dict(params)

    def setInputTypes(self, types):
        self.inout_types = {OUTPUT_NAME: types[INPUT_NAME]}

    def getOutputTypes(self):
        # output = input
        return self.inout_types

    def compute(self):
        '''
        Calls the processing function with the given parameters and one input
        stream.
        '''
        try:
            try:
                func, params = self.compute_fn
            except TypeError:
                func = self.compute_fn
                params = {}
            output = func(self, self.st, **params)
            self.outputstreams.append(output)
        except:
            self.log(traceback.format_exc())
            self.error += traceback.format_exc()
            self.log("Failed to execute function '%s' with parameters %s"
                     % (func.__name__, params))

from dispel4py.workflow_graph import WorkflowGraph


def createProcessingComposite(chain, suffix='',
                              controlParameters={},
                              provRecorder=None):
    '''
    Creates a composite PE wrapping a pipeline that processes obspy streams.
    :param chain: list of functions that process obspy streams. The function
    takes one input parameter, stream, and returns an output stream.
    :param requestId: id of the request that the stream is associated with
    :param controlParameters: environment parameters for the processing\
        elements
    :rtype: dictionary inputs and outputs of the composite PE that was\
        created
    '''
    prev = None
    first = None
    graph = WorkflowGraph()

    for fn_desc in chain:
        pe = ObspyStreamPE()
        try:
            fn = fn_desc[0]
            params = fn_desc[1]
        except TypeError:
            fn = fn_desc
            params = {}

        pe.compute_fn = fn
        pe.name = 'ObspyStreamPE_' + fn.__name__ + suffix
        pe.controlParameters = controlParameters
        pe.appParameters = dict(params)
        pe.setCompute(fn, params)

        # connect the metadata output to the provenance recorder PE
        # if there is one
        if provRecorder:
            graph.connect(pe, 'metadata', provRecorder, 'metadata')

        if prev:
            graph.connect(prev, OUTPUT_NAME, pe, INPUT_NAME)
        else:
            first = pe
        prev = pe

    # Map inputs and outputs of the wrapper to the nodes in the subgraph
    graph.inputmappings = {'input': (first, INPUT_NAME)}
    graph.outputmappings = {'output': (prev, OUTPUT_NAME)}

    return graph
