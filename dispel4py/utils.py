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
Collection of dispel4py utilities.
'''

from dispel4py.workflow_graph import WorkflowGraph

from importlib import import_module
from imp import load_source, new_module
import __builtin__
from types import ModuleType

class DummyModule(ModuleType):
    def __getattr__(self, key):
        return None
    __all__ = []   # support wildcard imports

def loadGraphIgnoreImports(module_name, graph_var):
    '''
    Loads a graph from the given module and ignores any import errors.
    '''

    def tryimport(name, globals={}, locals={}, fromlist=[], level=-1):
        try:
            return realimport(name, globals, locals, fromlist, level)
        except ImportError as exc:
            print "Warning: %s" % exc
            return DummyModule(name)

    realimport, __builtin__.__import__ = __builtin__.__import__, tryimport

    mod = import_module(module_name)
    graph = getattr(mod, graph_var)

    __builtin__.__import__ = realimport
    
    return graph
    
def loadSourceIgnoreImports(module_name, path, attr_name):
    '''
    Import a module from the given source file at 'path' and return the named attribute 'attr_name'.
    Any import errors are being ignored.
    
    :param module_name: name of the module to load
    :param path: location of the source file
    :param attr_name: name of the attribute within the module
    '''
    def tryimport(name, globals={}, locals={}, fromlist=[], level=-1):
        try:
            return realimport(name, globals, locals, fromlist, level)
        except ImportError as exc:
            print "Warning: %s" % exc
            return DummyModule(name)

    realimport, __builtin__.__import__ = __builtin__.__import__, tryimport

    mod = load_source(module_name, path)
    attr = getattr(mod, attr_name)
    
    __builtin__.__import__ = realimport
    
    return attr

def loadSource(module_name, path, attr_name):
    '''
    Import a module from the given source file at 'path' and return the named attribute 'attr_name'.
    
    :param module_name: name of the module to load
    :param path: location of the source file
    :param attr_name: name of the attribute within the module
    '''
    mod = load_source(module_name, path)
    attr = getattr(mod, attr_name)
    return attr

def loadIgnoreImports(module_name, attr_name, code):
    '''
    Import a module from source and return the specified attribute.
    
    :param module_name: name of the module to load
    :param attr_name: name of the attribute within the module
    :param code: source code of the module
    '''
    def tryimport(name, globals={}, locals={}, fromlist=[], level=-1):
        try:
            return realimport(name, globals, locals, fromlist, level)
        except ImportError as exc:
            print "Warning: %s" % exc
            return DummyModule(name)

    realimport, __builtin__.__import__ = __builtin__.__import__, tryimport

    mod = new_module(module_name)
    exec code in mod.__dict__
    attr = getattr(mod, attr_name)
    __builtin__.__import__ = realimport
    
    return attr
    
def loadGraph(module_name, attr=None):
    '''
    Loads a graph from the given module.
    '''
    mod = import_module(module_name)
    graph = None
    if attr is not None:
        # use the named attribute
        graph = getattr(mod, attr)
    else:
        # search for a workflow graph in the given module
        for i in dir(mod):
            attr = getattr(mod, i)
            if isinstance(attr, WorkflowGraph):
                graph = attr
    return graph

    
from sys import getsizeof
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

def total_size(o, handlers={}, verbose=False):
    """ 
    From: http://code.activestate.com/recipes/577504/
    Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)

import copy

def make_hash(o):
    
    """
    Makes a hash from a dictionary, list, tuple or set to any level, that contains
    only other hashable types (including any lists, tuples, sets, and
    dictionaries).
    """
    if isinstance(o, (set, tuple, list)):
        return hash(tuple([make_hash(e) for e in o]))

    if not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)

    return hash(tuple(frozenset(sorted(new_o.items()))))
