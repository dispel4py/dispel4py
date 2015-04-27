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

from imp import load_source, new_module
import __builtin__
from types import ModuleType

class DummyModule(ModuleType):
    def __getattr__(self, key):
        return None
    __all__ = []   # support wildcard imports


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
    
def extractAnnotations(fn):
    '''
    Extract and return method annotations according to the format::
    
        <Description - free text>
        @param <name> <type> <','> Free text description
        @return <type>
    '''
    ret = {'doc':'', 'params':[], 'return':''}
    if fn.__doc__ == None: return ret
    for l in fn.__doc__.splitlines():
        l = l.strip()
        if l == '': continue
        if not l.startswith('@'):
            ret['doc'] += l
        else:
            if l.startswith('@param'):
                toks = l.split(',')
                pdescr = ''
                if len(toks) == 2:
                    pdescr = toks[1].strip()
                lhstoks = toks[0].split()
                pname = lhstoks[1]
                ptype = ''
                if len(lhstoks) >= 3:
                    ptype = lhstoks[2]

                ret['params'].append({'name':pname, 'type':ptype, 'doc':pdescr})
            elif l.startswith('@return'):
                toks = l.split()
                ret['return'] = toks[1].strip()
    return ret
