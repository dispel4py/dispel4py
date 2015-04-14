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
Creates a Storm topology from a dispel4py graph.
'''

import json
import pickle

from storm import ttypes as tt

from dispel4py.workflow_graph import WorkflowNode
from dispel4py.core import GROUPING
from dispel4py.storm.utils import stormLogger

PE_WRAPPER = {'source': 'source_wrapper.py',
              'simple': 'simple_wrapper.py',
              'lockstep': 'lockstep_wrapper.py'}


def buildTopology(workflow):
    '''
    Builds a Storm topology from the given dispel4py workflow.
    '''
    workflow.propagate_types()
    graph = workflow.graph
    input_connections = {}
    input_mappings = {}
    node_names = {}

    node_counter = 1
    # give each node a unique name
    for node in graph.nodes():
        pe = node.getContainedObject()
        pe.log = stormLogger
        node_names[pe] = str(pe.__class__.__name__) + str(node_counter)
        node_counter += 1

    # now configure the connections
    for edge in graph.edges(data=True):
        direction = edge[2]['DIRECTION']
        source = direction[0]
        source_name = node_names[source]
        source_output = edge[2]['FROM_CONNECTION']
        dest = direction[1]
        dest_input = edge[2]['TO_CONNECTION']
        if dest not in input_connections:
            input_connections[dest] = {}
        if dest not in input_mappings:
            input_mappings[dest] = {}
        if source_name not in input_mappings[dest]:
            input_mappings[dest][source_name] = {}

        # find out the grouping of the input
        # shuffle grouping by default
        # this can be overridden by the PE implementation
        # or when creating the topology
        grouping = tt.Grouping(shuffle=tt.NullStruct())
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                fields = []
                try:
                    # try if the grouping type is a list of indexes
                    for i in groupingtype:
                        fields.append(
                            source.getOutputTypes()[source_output][i])
                except TypeError:
                    # hopefully the grouping type is a list of
                    # tuple element names
                    fields = groupingtype
                # fields grouping with the list of fields
                grouping = tt.Grouping(fields=fields)
            elif groupingtype == 'all':
                grouping = tt.Grouping(all=tt.NullStruct())
            elif groupingtype == 'none':
                grouping = tt.Grouping(none=tt.NullStruct())
        input_connections[dest][
            tt.GlobalStreamId(source_name, source_output)] = grouping
        input_mappings[dest][source_name][source_output] = dest_input

    # add bolts and spouts
    spout_specs = {}
    bolt_specs = {}
    for node, data in graph.nodes(data=True):
        pe = node.getContainedObject()
        pe_name = node_names[pe]
        print "Spec'ing %s" % pe_name
        # print pe
        # print pe.inputconnections
        # print pe.outputconnections
        # we're handling only PEs for now
        if node.nodeType != WorkflowNode.WORKFLOW_NODE_PE:
            raise Exception(
                "Unexpected workflow node of type '%s'" % node.nodeType)
        streams = {}
        for output, outtype in pe.getOutputTypes().iteritems():
            streams[output] = tt.StreamInfo(outtype, False)

        # name of the file created in temporary directory resources
        module_name = pe.__module__
        pe_config = {}
        for key, value in vars(pe).iteritems():
            if key not in pe.pickleIgnore:
                pe_config[key] = value

        json_conf = {'dispel4py.module': module_name,
                     'dispel4py.script': pe.__class__.__name__,
                     'dispel4py.inputmapping': {},
                     'dispel4py.config': pickle.dumps(pe_config)}
        if not pe.inputconnections or pe not in input_connections:
            # if there are no inputs it's a spout :P
            spout_specs[pe_name] = tt.SpoutSpec(
                spout_object=tt.ComponentObject(
                    shell=tt.ShellComponent("python", PE_WRAPPER['source'])),
                common=tt.ComponentCommon(
                    inputs={},
                    streams=streams,
                    json_conf=json.dumps(json_conf)))
        else:
            inputs = input_connections[pe] if pe in input_connections else {}
            json_conf['dispel4py.inputmapping'] = input_mappings[pe]\
                if pe in input_mappings else {}
            bolt_specs[pe_name] = tt.Bolt(
                bolt_object=tt.ComponentObject(
                    shell=tt.ShellComponent("python", PE_WRAPPER[pe.wrapper])),
                common=tt.ComponentCommon(
                    inputs=inputs,
                    streams=streams,
                    parallelism_hint=3,
                    json_conf=json.dumps(json_conf)))

    print "spouts %s" % spout_specs
    print "bolts  %s" % bolt_specs

    return tt.StormTopology(spouts=spout_specs,
                            bolts=bolt_specs,
                            state_spouts={})
