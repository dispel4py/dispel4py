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
The iPython extension for viewing and listing modules from the Dispel4Py registry, 
and visualising a Dispel4Py graph using Graphviz dot.
'''

from dispel4py import registry
from dispel4py import dispel4py_client
import traceback

def _initRegistry():
    dispel4py_client.config = dispel4py_client.configure()
    return dispel4py_client._initRegistry()
    
def _listPackages(pkg):
    reg = _initRegistry()
    try:
        pkgs = reg.listPackages(pkg)
        return [p for p in pkgs if not p.endswith('__impl') and not p.endswith('__gendef') and not p == pkg]
    except:
        objs = []

def _listObjects(name):
    reg = _initRegistry()
    try:
        objs = reg.list(name)
    except registry.UnknownPackageException:
        objs = []
    return objs
    
from verce.workflow_graph import drawDot
from IPython.core.display import display_png

def display(graph):
    '''
    Visualises the input graph.
    '''
    display_png(drawDot(graph), raw=True)    

from IPython.core.magic import (Magics, magics_class, line_magic)

@magics_class
class Dispel4PyMagics(Magics):
    '''
    Creates the dispel4py command in iPython, to be used interactively for training and other purposes.
    '''

    @line_magic
    def dispel4py(self, line):
        command = line.split()
        if command[0] == 'list': 
            pkgs = _listPackages(command[1])
            if pkgs: 
                print "Packages:"
                for p in pkgs: print '  ' + p
            objs = _listObjects(command[1])
            pes = []
            functions = []
            for obj in objs:
                 if obj['type'] == 'eu.verce.registry.domains.PESig':
                     pes.append(obj['name'])
                 if obj['type'] == 'eu.verce.registry.domains.FunctionSig':
                     functions.append(obj['name'])
            if pes: 
                print "Processing Elements:"
                for p in pes: print '  ' + p
            if functions:
                print "Functions:"
                for f in functions: print '  ' + f
        elif command[0] == 'view':
            try:
                reg = _initRegistry()
                source = reg.get_code(command[1])
                if source is None:
                    print "Resource '%s' not found\n" % name
                else:
                    print source
            except Exception as err:
                print traceback.format_exc()
                print "An error occurred."
        else:
            print "Unknown command '%s'" % line
            
def load_ipython_extension(ip):
    ip.register_magics(Dispel4PyMagics)

def unload_ipython_extension(ipython):
    # If you want your extension to be unloadable, put that logic here.
    None
