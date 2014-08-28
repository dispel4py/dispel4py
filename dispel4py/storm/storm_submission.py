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
Submits a Dispel4Py graph for processing to Storm. 
All dependencies must be available in the named resources directory or from the registry.

From the commandline, run the following::

    python -m dispel4py.storm.storm_submission module [name] [-h] -m {local,remote,create} [-r resourceDir] [-a attribute] [-s]

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
    do not remove Storm resources after submission (default is to remove resources)
    
.. note::
    A Storm topology, once submitted, runs forever until it is explicitly killed.
'''

import argparse
import datetime
import getpass
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from importlib import import_module

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from dispel4py.storm.topology import buildTopology
from dispel4py import registry
from dispel4py.utils import loadGraph
from dispel4py.workflow_graph import WorkflowGraph

TOPOLOGY_THRIFT_FILE = 'topology.thrift'
STORM_SUBMISSION_CLIENT = 'storm_submission_client.py'

def _mkdir_ifnotexists(path):
    try:
        os.mkdir(path)
    except OSError:
        pass

def createPackage(args, static_input):
    '''
    Creates a Storm submission package for the given dispel4py graph.
    
    :param module_name: name of the graph module that creates a graph
    :param attr: name of the graph attribute within the module - if None the first WorkflowGraph is used
    :param res: resource directory - if None the default is "resources"
    :rtype: name of the temporary directory that contains the submission package
    '''
    module_name = args.module
    attr = args.attr
    res = args.resources
    if res is None: res='resources'
    
    graph=loadGraph(module_name, attr)            
    # we don't want any nested subgraphs
    graph.flatten()

    # create a temporary directory 
    tmpdir = tempfile.mkdtemp()
    resources_dir = tmpdir + '/' + res

    # copy dependencies of PEs in the graph to resources in temp directory
    shutil.copytree(res, resources_dir)
    dispel4py_dir = resources_dir + '/dispel4py'
    _mkdir_ifnotexists(dispel4py_dir)
    _mkdir_ifnotexists(dispel4py_dir + '/storm')
    shutil.copy('dispel4py/__init__.py', dispel4py_dir)
    shutil.copy('dispel4py/core.py', dispel4py_dir)
    shutil.copy('dispel4py/base.py', dispel4py_dir)
    shutil.copy('dispel4py/__init__.py', dispel4py_dir + '/storm/')
    shutil.copy('dispel4py/storm/utils.py', dispel4py_dir + '/storm/')

    # copy client and dependencies for storm submission to the temp directory
    dispel4py_dir = tmpdir + '/dispel4py'
    _mkdir_ifnotexists(dispel4py_dir)
    _mkdir_ifnotexists(dispel4py_dir + '/storm')
    shutil.copy('dispel4py/__init__.py', dispel4py_dir)
    shutil.copy('dispel4py/__init__.py', dispel4py_dir + '/storm/')
    shutil.copy('dispel4py/storm/client.py', dispel4py_dir + '/storm/')
    shutil.copy('dispel4py/storm/storm_submission_client.py', tmpdir)
    shutil.copy('java/src/dispel4py/storm/ThriftSubmit.java', tmpdir + '/dispel4py/storm/')
    
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
    print "Sources: %s" % [ pe.id for pe in sources ]
    for pe in sources:
        pe._static_input = static_input
    
    # create the storm topology
    topology = buildTopology(graph)

    # cache PE dependencies imported from the registry to resources_dir
    registry.createResources(resources_dir, registry.currentRegistry())

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
        print 'Error: Please provide the installation directory of Storm as environment variable STORM_HOME'
        sys.exit(1)

def submit(args, inputs):
    '''
    Creates a Storm submission package and submits it to a remote cluster.
    
    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None the first WorkflowGraph is used
    :param res: resource directory
    :param save: if True the Storm submission package is not deleted at the end of the run
    '''
    STORM_HOME = _getStormHome()
    topologyName = args.name
    tmpdir = createPackage(args, inputs)
    print 'Created Storm submission package in %s' % tmpdir
    # javacmd = 'javac', '-cp', '.:%s/lib/*:%s/*' % (STORM_HOME, STORM_HOME), 'eu/dispel4py/storm/ThriftSubmit.java'
    # try:
    #     proc = subprocess.Popen(javacmd, cwd=tmpdir)
    #     proc.wait()
    # except:
    #     pass
    try:
        print "Submitting topology '%s' to Storm" % topologyName
        stormshell = '%s/bin/storm' % STORM_HOME, 'shell','resources/', 'python', 'storm_submission_client.py', topologyName
        proc = subprocess.Popen(stormshell, cwd=tmpdir)
        proc.wait()
    except:
        pass
    if args.save:
        print tmpdir
    else:
        shutil.rmtree(tmpdir)
        print 'Deleted %s' % tmpdir
        
def runLocal(args, inputs):
    '''
    Creates a Storm submission package and executes it locally.
    Note that the Storm topology runs until the process is explicitly killed, for example by pressing Ctrl-C.
    
    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None the first WorkflowGraph is used
    :param res: resource directory
    :param save: if True the Storm submission package is not deleted at the end of the run
    '''
    STORM_HOME = _getStormHome()
    topologyName = args.name
    tmpdir = createPackage(args, inputs)
    print 'Created Storm submission package in %s' % tmpdir
    try:
        print 'Compiling java client'
        javacp = '.:%s/lib/*:%s/*' % (STORM_HOME, STORM_HOME)
        javacmd = 'javac', '-cp', javacp, 'dispel4py/storm/ThriftSubmit.java'
        proc = subprocess.Popen(javacmd, cwd=tmpdir)
        proc.wait()
        print 'Running topology in local mode'
        javacmd = 'java', '-cp', javacp, 'dispel4py.storm.ThriftSubmit', 'topology.thrift', topologyName
        proc = subprocess.Popen(javacmd, cwd=tmpdir)
        proc.wait()
    except:
        print traceback.format_exc()
        if args.save:
            print tmpdir
        else:
            shutil.rmtree(tmpdir)
            print 'Deleted %s' % tmpdir
            
def create(args):
    '''
    Creates a Storm submission package and prints the temp directory containing the package.
    
    :param mod: module that creates a dispel4py graph
    :param attr: name of graph attribute within the module - if None the first WorkflowGraph is used
    :param res: resource directory
    '''
    tmpdir = createPackage(args)
    print 'Created Storm submission package in %s' % tmpdir    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Submit a dispel4py graph for processing to Storm. All dependencies must be available in the named resources directory or from the registry.')
    parser.add_argument('module', help='module that creates a dispel4py graph')
    defaultName = getpass.getuser() + '_' + datetime.datetime.today().strftime('%Y%m%dT%H%M%S')
    parser.add_argument('name', nargs='?', default=defaultName, help='name of Storm topology to submit')
    parser.add_argument('-m', '--mode', help='execution mode', choices=['local', 'remote', 'create'], required=True)
    parser.add_argument('-r', '--resources', metavar='resourceDir', help='path to local modules used by the graph - default "./resources/" ')
    parser.add_argument('-a', '--attr', metavar='attribute', help='name of graph variable in the module')
    parser.add_argument('-s', '--save', help='do not remove Storm resources after submission', action='store_true')
    parser.add_argument('-f', '--file', metavar='inputfile', help='file containing the input dataset in JSON format')
    parser.add_argument('-i', '--iter', metavar='iterations', type=int, help='number of iterations')
    args = parser.parse_args()
    
    inputs = None
    if args.file:
        try:
            with open(args.file) as inputfile:
                inputs = json.loads(inputfile.read())
            print("Processing input file %s" % args.file)
        except:
            print traceback.format_exc()
            print('Cannot read input file %s' % args.file)
            sys.exit(1)
    elif args.iter > 0:
        inputs = [ {} for i in range(args.iter) ]
        print("Processing %s iterations" % args.iter)
    if type(inputs) != list:
        inputs = [inputs]
    
    if args.mode == 'local':
        runLocal(args, inputs)
    elif args.mode == 'remote':
        submit(args, inputs)
    elif args.mode == 'create':
        create(args)
    