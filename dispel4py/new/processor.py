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
Enactment of dispel4py graphs.
This module contains methods that are used by different mappings.

From the commandline, run the following command::

    dispel4py <mapping> <module>  [-h] [-a attribute] [-f inputfile]\
                                  [-i iterations] [...]

with parameters

:mapping:   target mapping
:module:    module that creates a Dispel4Py graph
:-n num:    number of processes (required)
:-a attr:   name of the graph attribute within the module (optional)
:-f file:   file containing input data in JSON format (optional)
:-i iter:   number of iterations to compute (default is 1)
:-h:        print this help page

Other parameters might be required by the target mapping, for example the
number of processes if running in a parallel environment.

'''

import argparse
import os
import sys
import types

from dispel4py.core import GROUPING
from dispel4py.utils import make_hash
from dispel4py.new.mappings import config

STATUS_ACTIVE = 10
STATUS_INACTIVE = 11
STATUS_TERMINATED = 12
# mapping for name to value
STATUS = {STATUS_ACTIVE: 'ACTIVE',
          STATUS_INACTIVE: 'INACTIVE',
          STATUS_TERMINATED: 'TERMINATED'}


def simpleLogger(self, msg):
    try:
        print "%s (rank %s): %s" % (self.id, self.rank, msg)
    except:
        print "%s: %s" % (self.id, msg)


def get_inputs(pe, inputs):
    try:
        return inputs[pe]
    except KeyError:
        pass
    try:
        return inputs[pe.name]
    except:
        pass
    try:
        return inputs[pe.id]
    except:
        pass


class GenericWriter(object):
    def __init__(self, wrapper, name):
        self.wrapper = wrapper
        self.name = name

    def write(self, data):
        self.wrapper._write(self.name, data)


class GenericWrapper(object):

    def __init__(self, pe):
        self.pe = pe
        self.pe.wrapper = self
        for o in self.pe.outputconnections:
            self.pe.outputconnections[o]['writer'] = GenericWriter(self, o)
        self.targets = {}
        self._sources = {}

    @property
    def sources(self):
        return self._sources

    @sources.setter
    def sources(self, sources):
        # count and store number of inputs when setting the sources
        num_inputs = 0
        for i in sources.values():
            num_inputs += len(i)
        self._num_sources = num_inputs
        self._sources = sources

    def process(self):
        num_iterations = 0
        self.pe.preprocess()
        result = self._read()
        inputs, status = result
        # self.pe.log('Result: %s, status=%s' % (inputs, STATUS[status]))
        while status != STATUS_TERMINATED:
            if inputs is not None:
                outputs = self.pe.process(inputs)
                num_iterations += 1
                if outputs is not None:
                    # self.pe.log('Produced output: %s' % outputs)
                    for key, value in outputs.iteritems():
                        self._write(key, value)
            inputs, status = self._read()
            # self.pe.log('Result: %s, status=%s' % (inputs, STATUS[status]))
        self.pe.postprocess()
        self._terminate()
        if num_iterations == 1:
            self.pe.log('Processed 1 iteration.')
        else:
            self.pe.log('Processed %s iterations.' % num_iterations)

    def _read(self):
        # check the provided inputs
        if self.provided_inputs is not None:
            if isinstance(self.provided_inputs, (int, long)) and \
                    self.provided_inputs > 0:
                self.provided_inputs -= 1
                return {}, STATUS_ACTIVE
            elif self.provided_inputs:
                return self.provided_inputs.pop(0), STATUS_ACTIVE
            else:
                return None, STATUS_TERMINATED

    def _write(self, name, data):
        None

    def _terminate(self):
        None


def _wrapper_write(self, name, data):
    self.wrapper._write(name, data)


class ShuffleCommunication(object):
    def __init__(self, rank, sources, destinations):
        self.destinations = destinations
        self.currentIndex = (sources.index(rank) % len(self.destinations)) - 1
        self.name = None

    def getDestination(self, data):
        self.currentIndex = (self.currentIndex+1) % len(self.destinations)
        return [self.destinations[self.currentIndex]]


class GroupByCommunication(object):
    def __init__(self, destinations, input_name, groupby):
        self.groupby = groupby
        self.destinations = destinations
        self.input_name = input_name
        self.name = groupby

    def getDestination(self, data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        dest_index = abs(make_hash(output)) % len(self.destinations)
        return [self.destinations[dest_index]]


class AllToOneCommunication(object):
    def __init__(self, destinations):
        self.destinations = destinations
        self.name = 'global'

    def getDestination(self, data):
        return [self.destinations[0]]


class OneToAllCommunication(object):
    def __init__(self, destinations):
        self.destinations = destinations
        self.name = 'all'

    def getDestination(self, data):
        return self.destinations


def _getConnectedInputs(node, graph):
    names = []
    for edge in graph.edges(node, data=True):
        direction = edge[2]['DIRECTION']
        dest = direction[1]
        dest_input = edge[2]['TO_CONNECTION']
        if dest == node.getContainedObject():
            names.append(dest_input)
    return names


def _getNumProcesses(size, numSources, numProcesses, totalProcesses):
    div = max(1, totalProcesses-numSources)
    return int(numProcesses * (size - numSources)/div)


def _assign_processes(workflow, size):
    graph = workflow.graph
    processes = {}
    success = True
    totalProcesses = 0
    numSources = 0
    sources = []
    for node in graph.nodes():
        pe = node.getContainedObject()
        # if pe.inputconnections:
        if _getConnectedInputs(node, graph):
            totalProcesses = totalProcesses + pe.numprocesses
        else:
            sources.append(pe.id)
            totalProcesses += 1
            numSources += 1

    if totalProcesses > size:
        success = False
        # we need at least one process for each node in the graph
        print 'Graph is larger than job size: %s > %s.' %\
            (totalProcesses, size)
    else:
        node_counter = 0
        for node in graph.nodes():
            pe = node.getContainedObject()
            prcs = 1 if pe.id in sources else _getNumProcesses(
                size, numSources, pe.numprocesses, totalProcesses)
            processes[pe.id] = range(node_counter, node_counter+prcs)
            node_counter = node_counter + prcs
    return success, sources, processes


def _getCommunication(rank, source_processes,
                      dest, dest_input, dest_processes):
    communication = ShuffleCommunication(
        rank, source_processes, dest_processes)
    try:
        if GROUPING in dest.inputconnections[dest_input]:
            groupingtype = dest.inputconnections[dest_input][GROUPING]
            if isinstance(groupingtype, list):
                communication = GroupByCommunication(
                    dest_processes, dest_input, groupingtype)
            elif groupingtype == 'all':
                communication = OneToAllCommunication(dest_processes)
            elif groupingtype == 'global':
                communication = AllToOneCommunication(dest_processes)
    except KeyError:
        print("No input '%s' defined for PE '%s'" % (dest_input, dest.id))
        raise
    return communication


def _create_connections(graph, node, processes):
    pe = node.getContainedObject()
    inputmappings = {i: {} for i in processes[pe.id]}
    outputmappings = {i: {} for i in processes[pe.id]}
    for edge in graph.edges(node, data=True):
        direction = edge[2]['DIRECTION']
        source = direction[0]
        source_output = edge[2]['FROM_CONNECTION']
        dest = direction[1]
        dest_processes = list(processes[dest.id])
        source_processes = list(processes[source.id])
        dest_input = edge[2]['TO_CONNECTION']
        allconnections = edge[2]['ALL_CONNECTIONS']
        if dest == pe:
            for i in processes[pe.id]:
                for (source_output, dest_input) in allconnections:
                    try:
                        inputmappings[i][dest_input] += source_processes
                    except KeyError:
                        inputmappings[i][dest_input] = source_processes
        if source == pe:
            for i in processes[pe.id]:
                for (source_output, dest_input) in allconnections:
                    communication = _getCommunication(
                        i, source_processes, dest, dest_input, dest_processes)
                    try:
                        outputmappings[i][source_output].append(
                            (dest_input, communication))
                    except KeyError:
                        outputmappings[i][source_output] = \
                            [(dest_input, communication)]
    return inputmappings, outputmappings


def _connect(workflow, processes):
    graph = workflow.graph
    outputmappings = {}
    inputmappings = {}
    for node in graph.nodes():
        inc, outc = _create_connections(graph, node, processes)
        inputmappings.update(inc)
        outputmappings.update(outc)
    return inputmappings, outputmappings


def assign_and_connect(workflow, size):
    success, sources, processes = _assign_processes(workflow, size)
    if success:
        inputmappings, outputmappings = _connect(workflow, processes)
        return processes, inputmappings, outputmappings
    else:
        return None

import copy

from dispel4py.workflow_graph import WorkflowGraph


def get_partitions(workflow):
    try:
        partitions = workflow.partitions
    except AttributeError:
        sourcePartition = []
        otherPartition = []
        graph = workflow.graph
        for node in graph.nodes():
            pe = node.getContainedObject()
            if not _getConnectedInputs(node, graph):
                sourcePartition.append(pe)
            else:
                otherPartition.append(pe)
        partitions = [sourcePartition, otherPartition]
        workflow.partitions = partitions
    return partitions


def create_partitioned(workflow_all):
    processes_all, inputmappings_all, outputmappings_all = \
        assign_and_connect(workflow_all, len(workflow_all.graph.nodes()))
    proc_to_pe_all = {v[0]: k for k, v in processes_all.iteritems()}
    partitions = get_partitions(workflow_all)
    external_connections = []
    pe_to_partition = {}
    partition_pes = []
    for i in range(len(partitions)):
        for pe in partitions[i]:
            pe_to_partition[pe.id] = i
    for index in range(len(partitions)):
        result_mappings = {}
        part = partitions[index]
        partition_id = index
        component_ids = [pe.id for pe in part]
        workflow = copy.deepcopy(workflow_all)
        graph = workflow.graph
        for node in graph.nodes():
            if node.getContainedObject().id not in component_ids:
                graph.remove_node(node)
        processes, inputmappings, outputmappings = \
            assign_and_connect(workflow, len(graph.nodes()))
        proc_to_pe = {}
        for node in graph.nodes():
            pe = node.getContainedObject()
            proc_to_pe[processes[pe.id][0]] = pe
        for node in graph.nodes():
            pe = node.getContainedObject()
            pe.rank = index
            proc_all = processes_all[pe.id][0]
            for output_name in outputmappings_all[proc_all]:
                for dest_input, comm_all in\
                        outputmappings_all[proc_all][output_name]:
                    dest = proc_to_pe_all[comm_all.destinations[0]]
                    if dest not in processes:
                        # it's an external connection
                        external_connections.append((comm_all,
                                                     partition_id,
                                                     pe.id,
                                                     output_name,
                                                     pe_to_partition[dest],
                                                     dest, dest_input))
                        try:
                            result_mappings[pe.id].append(output_name)
                        except:
                            result_mappings[pe.id] = [output_name]
        partition_pe = SimpleProcessingPE(inputmappings,
                                          outputmappings,
                                          proc_to_pe)

        # use number of processes if specified in graph
        try:
            partition_pe.numprocesses = workflow_all.numprocesses[partition_id]
        except:
            # use default assignment of processes
            pass

        partition_pe.workflow = workflow
        partition_pe.partition_id = partition_id
        if result_mappings:
            partition_pe.result_mappings = result_mappings
        partition_pe.map_inputs = _map_inputs_to_pes
        partition_pe.map_outputs = _map_outputs_from_pes
        partition_pes.append(partition_pe)
    # print 'EXTERNAL CONNECTIONS : %s' % external_connections
    ubergraph = WorkflowGraph()
    ubergraph.pe_to_partition = pe_to_partition
    ubergraph.partition_pes = partition_pes
    # sort the external connections so that nodes are added in the same order
    # if doing this in multiple processes in parallel this is important
    for comm, source_partition, source_id, source_output, dest_partition, \
            dest_id, dest_input in sorted(external_connections):
        partition_pes[source_partition]._add_output((source_id, source_output))
        partition_pes[dest_partition]._add_input((dest_id, dest_input),
                                                 grouping=comm.name)
        ubergraph.connect(partition_pes[source_partition],
                          (source_id, source_output),
                          partition_pes[dest_partition],
                          (dest_id, dest_input))
    return ubergraph


def map_inputs_to_partitions(ubergraph, inputs):
    mapped_input = {}
    for pe in inputs:
        try:
            partition_id = ubergraph.pe_to_partition[pe]
            pe_id = pe
        except:
            partition_id = ubergraph.pe_to_partition[pe.id]
            pe_id = pe.id
        mapped_pe = ubergraph.partition_pes[partition_id]
        try:
            mapped_pe_input = []
            for i in inputs[pe]:
                mapped_data = {(pe_id, name):
                               [data] for name, data in i.iteritems()}
                mapped_pe_input.append(mapped_data)
        except TypeError:
            mapped_pe_input = inputs[pe]
        mapped_input[mapped_pe] = mapped_pe_input
    return mapped_input


def _map_inputs_to_pes(data):
    result = {}
    for i in data:
        pe_id, input_name = i
        mapped_data = [{input_name: block} for block in data[i]]
        try:
            result[pe_id].update(mapped_data)
        except KeyError:
            result[pe_id] = mapped_data
    return result


def _map_outputs_from_pes(data):
    result = {}
    for pe_id in data:
        for i in data[pe_id]:
            result[(pe_id, i)] = data[pe_id][i]
    return result


def _no_map(data):
    return data


from dispel4py.core import GenericPE


def _is_root(node, workflow):
    result = True
    pe = node.getContainedObject()
    for edge in workflow.graph[node].values():
        if pe == edge['DIRECTION'][1]:
            result = False
            break
    return result


def _get_dependencies(proc, inputmappings):
    dep = []
    for input_name, sources in inputmappings[proc].iteritems():
        for s in sources:
            sdep = _get_dependencies(s, inputmappings)
            for n in sdep:
                if n not in dep:
                    dep.append(n)
            dep.append(s)
    return dep


def _order_by_dependency(inputmappings, outputmappings):
    ordered = []
    for proc in outputmappings:
        if not outputmappings[proc]:
            dep = _get_dependencies(proc, inputmappings)
            for n in ordered:
                try:
                    dep.remove(n)
                except:
                    # never mind if the element wasn't in the list
                    pass
            ordered += dep
            ordered.append(proc)
    return ordered


class SimpleProcessingPE(GenericPE):
    '''
    A PE that processes a subgraph of PEs in sequence.
    '''
    def __init__(self, input_mappings, output_mappings, proc_to_pe):
        GenericPE.__init__(self)
        # work out the order of PEs
        self.ordered = _order_by_dependency(input_mappings, output_mappings)
        self.input_mappings = input_mappings
        self.output_mappings = output_mappings
        self.proc_to_pe = proc_to_pe
        self.result_mappings = None
        self.map_inputs = _no_map
        self.map_outputs = _no_map

    def _preprocess(self):
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            try:
                pe.rank = self.rank
            except:
                pass
            pe.log = types.MethodType(simpleLogger, pe)
            pe.preprocess()

    def _postprocess(self):
        all_inputs = {}
        results = {}
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.writer = SimpleWriter(self, pe,
                                     self.output_mappings[proc],
                                     self.result_mappings)
            pe._write = types.MethodType(_simple_write, pe)
            # if there was data produced in postprocessing
            # then we need to process that data in the PEs downstream
            if proc in all_inputs:
                for data in all_inputs[proc]:
                    # pe.log('Processing input: %s' % data)
                    result = pe.process(data)
                    # pe.log('Produced result: %s' % result)
                    if result is not None:
                        for output_name in result:
                            pe.write(output_name, result[output_name])
            # once all the input data is processed this PE can finish
            pe.postprocess()
            # PE might write data during postprocessing
            for p, input_data in pe.writer.all_inputs.iteritems():
                try:
                    all_inputs[p].extend(input_data)
                except:
                    all_inputs[p] = input_data
            if pe.writer.results:
                results[pe] = pe.writer.results
            pe.writer.all_inputs = {}
            pe.writer.results = {}
        results = self.map_outputs(results)
        for key, value in results.iteritems():
            self._write(key, value)

    def _process(self, inputs):
        all_inputs = {}
        results = {}
        inputs = self.map_inputs(inputs)
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.writer = SimpleWriter(self, pe,
                                     self.output_mappings[proc],
                                     self.result_mappings)
            pe._write = types.MethodType(_simple_write, pe)
            provided_inputs = get_inputs(pe, inputs)
            try:
                other_inputs = all_inputs[proc]
                try:
                    provided_inputs.append(other_inputs)
                except:
                    provided_inputs = other_inputs
            except:
                pass

            if isinstance(provided_inputs, (int, long)):
                for i in xrange(0, provided_inputs):
                    _process_data(pe, {})
            else:

                if provided_inputs is None:
                    if not pe.inputconnections:
                        # run at least once for a source of the graph
                        provided_inputs = [{}]
                    else:
                        # no data
                        provided_inputs = []

                for data in provided_inputs:
                    _process_data(pe, data)

            for p, input_data in pe.writer.all_inputs.iteritems():
                try:
                    all_inputs[p].extend(input_data)
                except:
                    all_inputs[p] = input_data
            if pe.writer.results:
                results[pe.id] = pe.writer.results
            # discard data from the PE writer
            pe.writer.all_inputs = {}
            pe.writer.results = {}
        results = self.map_outputs(results)
        return results


def _process_data(pe, data):
    # pe.log('Processing input: %s' % data)
    result = pe.process(data)
    # pe.log('Produced result: %s' % result)
    if result is not None:
        for output_name in result:
            pe.write(output_name, result[output_name])


def _simple_write(self, name, data):
    self.writer.write(name, data)


class SimpleWriter(object):
    def __init__(self, simple_pe, pe, output_mappings, result_mappings=None):
        self.simple_pe = simple_pe
        self.pe = pe
        self.output_mappings = output_mappings
        self.result_mappings = result_mappings
        self.all_inputs = {}
        self.results = {}

    def write(self, output_name, data):
        # self.pe.log('Writing %s to %s' % (data, output_name))
        try:
            destinations = self.output_mappings[output_name]
            for input_name, comm in destinations:
                for p in comm.destinations:
                    input_data = {input_name: data}
                    try:
                        self.all_inputs[p].append(input_data)
                    except:
                        self.all_inputs[p] = [input_data]
        except KeyError:
            # no destinations for this output
            # if there are no named result outputs
            # the data is added to the results of the PE
            if self.result_mappings is None:
                self.simple_pe.wrapper._write((self.pe.id, output_name),
                                              [data])
        # now check if the output is in the named results
        # (in case of a Tee) then data gets written to the PE results as well
        try:
            if output_name in self.result_mappings[self.pe.id]:
                self.simple_pe.wrapper._write((self.pe.id, output_name),
                                              [data])
        except:
            pass


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph for processing.')
    parser.add_argument('target', help='target execution platform')
    parser.add_argument('module', help='module that creates a dispel4py graph '
                        '(python module or file name)')
    parser.add_argument('-a', '--attr', metavar='attribute',
                        help='name of graph variable in the module')
    parser.add_argument('-f', '--file', metavar='inputfile',
                        help='file containing input dataset in JSON format')
    parser.add_argument('-d', '--data', metavar='inputdata',
                        help='input dataset in JSON format')
    parser.add_argument('-i', '--iter', metavar='iterations', type=int,
                        help='number of iterations', default=1)
    return parser


def create_inputs(args, graph):
    import json
    inputs = {}

    if args.file:
        if not os.path.exists(args.file):
            raise ValueError("File '%s' does not exist." % args.file)
        try:
            with open(args.file) as inputfile:
                inputs = json.loads(inputfile.read())
        except Exception as e:
            print "Error reading JSON file '%s': %s" % (args.file, str(e))
            sys.exit(1)
    elif args.data:
        inputs = json.loads(args.data)
    else:
        if args.iter == 1:
            print 'Processing 1 iteration.'
        else:
            print 'Processing %s iterations.' % args.iter
        for node in graph.graph.nodes():
            if _is_root(node, graph):
                inputs[node.getContainedObject()] = args.iter

    # map input names to ids if necessary
    for node in graph.graph.nodes():
        pe = node.getContainedObject()
        try:
            d = inputs.pop(pe)
            inputs[pe.id] = d
        except:
            pass
        try:
            d = inputs.pop(pe.name)
            inputs[pe.id] = d
        except:
            pass

    return inputs


def load_graph_and_inputs(args):
    from dispel4py.utils import load_graph

    graph = load_graph(args.module, args.attr)
    if graph is None:
        return None, None

    graph.flatten()
    inputs = create_inputs(args, graph)
    return graph, inputs


def parse_common_args():
    parser = create_arg_parser()
    return parser.parse_known_args()


def main():
    from importlib import import_module

    args, remaining = parse_common_args()
    graph, inputs = load_graph_and_inputs(args)
    if graph is None:
        return

    try:
        # see if platform is in the mappings file as a simple name
        target = config[args.target]
    except KeyError:
        # it is a proper module name - fingers crossed...
        target = args.target
    try:
        parse_args = getattr(import_module(target), 'parse_args')
        args = parse_args(remaining, args)
    except SystemExit:
        # the sub parser raised an error
        raise
    except:
        # no other arguments required for target
        pass
    process = getattr(import_module(target), 'process')
    error_message = process(graph, inputs=inputs, args=args)
    if error_message:
        print error_message

if __name__ == "__main__":
    main()
