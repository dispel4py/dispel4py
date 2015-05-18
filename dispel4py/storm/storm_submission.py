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
Submits a dispel4py graph for processing to Storm.
All dependencies must be available in the named resources directory or from the
registry.

From the commandline, run the following::

    dispel4py storm <module> [name] [-h] -m {local,remote,create}
                             [-r resourceDir] [-a attribute] [-s]

with positional arguments:

:module: module that creates a dispel4py graph
:name:   name of Storm topology to submit (optional)

and optional arguments:

-h, --help
    show this help message and exit
-m, --mode mode
    execution mode, one of {local, remote, create}
-r, --resources resourceDir
    path to local modules used by the graph - default "./resources/" (optional)
-a, --attr attribute
    name of graph variable in the module (optional)
-s, --save
    do not remove Storm resources after submission (default is to remove
    resources)

.. note::
    A Storm topology, once submitted, runs forever until it is explicitly
    killed.
'''

import argparse
import datetime
import getpass
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from dispel4py.storm.topology import buildTopology

TOPOLOGY_THRIFT_FILE = 'topology.thrift'
STORM_SUBMISSION_CLIENT = 'storm_submission_client.py'

ROOT_DIR = os.path.abspath(os.path.join(os.path.join(
    os.path.join(__file__, os.pardir), os.pardir), os.pardir)) + '/'


def _mkdir_ifnotexists(path):
    try:
        os.mkdir(path)
    except OSError:
        pass


def createPackage(graph, args, static_input):
    '''
    Creates a Storm submission package for the given dispel4py graph.

    :param module_name: name of the graph module that creates a graph
    :param attr: name of the graph attribute within the module - if None the
    first WorkflowGraph is used
    :param res: resource directory - if None the default is "resources"
    :rtype: name of the temporary directory that contains the submission
    package
    '''
    print ROOT_DIR

    res = args.resources
    if res is None:
        res = 'resources'

    # create a temporary directory
    tmpdir = tempfile.mkdtemp()
    tmp_resources_dir = tmpdir + '/' + res

    # copy dependencies of PEs in the graph to resources in temp directory
    shutil.copytree(res, tmp_resources_dir)
    tmp_dispel4py_dir = tmp_resources_dir + '/dispel4py'
    _mkdir_ifnotexists(tmp_dispel4py_dir)
    _mkdir_ifnotexists(tmp_dispel4py_dir + '/storm')
    shutil.copy(ROOT_DIR + 'dispel4py/__init__.py', tmp_dispel4py_dir)
    shutil.copy(ROOT_DIR + 'dispel4py/core.py', tmp_dispel4py_dir)
    shutil.copy(ROOT_DIR + 'dispel4py/base.py', tmp_dispel4py_dir)
    shutil.copy(ROOT_DIR + 'dispel4py/workflow_graph.py', tmp_dispel4py_dir)
    shutil.copy(ROOT_DIR + 'dispel4py/storm/__init__.py',
                tmp_dispel4py_dir + '/storm/')
    shutil.copy(ROOT_DIR + 'dispel4py/storm/utils.py',
                tmp_dispel4py_dir + '/storm/')

    # copy client and dependencies for storm submission to the temp directory
    dispel4py_dir = tmpdir + '/dispel4py'
    _mkdir_ifnotexists(dispel4py_dir)
    _mkdir_ifnotexists(dispel4py_dir + '/storm')
    shutil.copy(ROOT_DIR + 'dispel4py/__init__.py', dispel4py_dir)
    shutil.copy(ROOT_DIR + 'dispel4py/__init__.py', dispel4py_dir + '/storm/')
    shutil.copy(ROOT_DIR + 'dispel4py/storm/client.py',
                dispel4py_dir + '/storm/')
    shutil.copy(ROOT_DIR + 'dispel4py/storm/storm_submission_client.py',
                tmpdir)
    shutil.copy(ROOT_DIR + 'java/src/dispel4py/storm/ThriftSubmit.java',
                tmpdir + '/dispel4py/storm/')
    try:
        shutil.copytree(ROOT_DIR + 'storm', tmpdir + '/storm')
    except:
        pass

    sources = []
    for node in graph.graph.nodes():
        pe = node.getContainedObject()
        is_source = True
        for edge in graph.graph.edges(node, data=True):
            if pe == edge[2]['DIRECTION'][1]:
                is_source = False
                break
        if is_source:
            sources.append(pe)
    print "Sources: %s" % [s.id for s in sources]
    for pe in sources:
        pe._static_input = static_input[pe.id]
        pe._num_iterations = args.iter

    # create the storm topology
    topology = buildTopology(graph)

    # cache PE dependencies imported from the registry to resources_dir
    # registry.createResources(resources_dir, registry.currentRegistry())

    # write thrift representation of the topology to a file
    transportOut = TTransport.TMemoryBuffer()
    protocolOut = TBinaryProtocol.TBinaryProtocol(transportOut)
    topology.write(protocolOut)
    bytes = transportOut.getvalue()
    with open(tmpdir+'/'+TOPOLOGY_THRIFT_FILE, "w") as thrift_file:
        thrift_file.write(bytes)
    return tmpdir


def _getStormHome():
    try:
        STORM_HOME = os.environ['STORM_HOME']
        return STORM_HOME
    except KeyError:
        print 'Error: Please provide the installation directory of Storm as \
               environment variable STORM_HOME'
        sys.exit(1)


def submit(workflow, args, inputs):
    '''
    Creates a Storm submission package and submits it to a remote cluster.

    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None the first
    WorkflowGraph is used
    :param res: resource directory
    :param save: if True the Storm submission package is not deleted at the end
    of the run
    '''
    STORM_HOME = _getStormHome()
    topologyName = args.name
    tmpdir = createPackage(workflow, args, inputs)
    print 'Created Storm submission package in %s' % tmpdir
    try:
        print "Submitting topology '%s' to Storm" % topologyName
        stormshell = '%s/bin/storm' % STORM_HOME, 'shell',\
                     'resources/',\
                     'python', 'storm_submission_client.py',\
                     topologyName
        proc = subprocess.Popen(stormshell, cwd=tmpdir)
        proc.wait()
    except:
        pass
    if args.save:
        print tmpdir
    else:
        shutil.rmtree(tmpdir)
        print 'Deleted %s' % tmpdir


def runLocal(workflow, args, inputs):
    '''
    Creates a Storm submission package and executes it locally.
    Note that the Storm topology runs until the process is explicitly killed,
    for example by pressing Ctrl-C.

    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None the first
    WorkflowGraph is used
    :param res: resource directory
    :param save: if True the Storm submission package is not deleted at the
    end of the run
    '''
    STORM_HOME = _getStormHome()
    topologyName = args.name
    tmpdir = createPackage(workflow, args, inputs)
    print 'Created Storm submission package in %s' % tmpdir
    try:
        print 'Compiling java client'
        javacp = '.:%s/lib/*:%s/*' % (STORM_HOME, STORM_HOME)
        javacmd = 'javac', '-cp', javacp, 'dispel4py/storm/ThriftSubmit.java'
        proc = subprocess.Popen(javacmd, cwd=tmpdir)
        proc.wait()
        print 'Running topology in local mode'
        javacmd = 'java', '-cp', javacp,\
                  'dispel4py.storm.ThriftSubmit',\
                  'topology.thrift',\
                  topologyName
        proc = subprocess.Popen(javacmd, cwd=tmpdir)
        proc.wait()
    except:
        print traceback.format_exc()
        if args.save:
            print tmpdir
        else:
            shutil.rmtree(tmpdir)
            print 'Deleted %s' % tmpdir


def create(workflow, args, inputs):
    '''
    Creates a Storm submission package and prints the temp directory containing
    the package.

    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None
    the first WorkflowGraph is used
    :param res: resource directory
    '''
    tmpdir = createPackage(workflow, args, inputs)
    print 'Created Storm submission package in %s' % tmpdir


def process(workflow, inputs, args=None):
    if args.mode == 'local':
        runLocal(workflow, args, inputs)
    elif args.mode == 'remote':
        submit(workflow, args, inputs)
    elif args.mode == 'create':
        create(workflow, args, inputs)


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph for processing to Storm.')
    defaultName = getpass.getuser() + '_' + \
        datetime.datetime.today().strftime('%Y%m%dT%H%M%S')
    parser.add_argument('name', nargs='?', default=defaultName,
                        help='name of Storm topology to submit')
    parser.add_argument('-m', '--mode',
                        help='execution mode',
                        choices=['local', 'remote', 'create'],
                        required=True)
    parser.add_argument('-r', '--resources', metavar='resourceDir',
                        help='path to local modules used by the graph \
                              - default "./resources/" ')
    parser.add_argument('-s', '--save',
                        help='do not remove Storm resources after submission',
                        action='store_true')
    result = parser.parse_args(args, namespace)
    return result


# if __name__ == "__main__":
#     parser = get_arg_parser()
#     args = parser.parse_args()
#
#     inputs = None
#     num_iterations = None
#     if args.file:
#         try:
#             with open(args.file) as inputfile:
#                 inputs = json.loads(inputfile.read())
#             print("Processing input file %s" % args.file)
#             if type(inputs) != list:
#                 inputs = [inputs]
#         except:
#             print traceback.format_exc()
#             print('Cannot read input file %s' % args.file)
#             sys.exit(1)
#     elif args.iter > 0:
#         #inputs = [ {} for i in range(args.iter) ]
#         print("Processing %s iterations" % args.iter)
#         num_iterations = args.iter
#
#     if args.mode == 'local':
#         runLocal(args, inputs, num_iterations)
#     elif args.mode == 'remote':
#         submit(args, inputs, num_iterations)
#     elif args.mode == 'create':
#         create(args)
#
