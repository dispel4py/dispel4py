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

import sys
import os
import json
import inspect
import tempfile
import subprocess
import traceback

from dispel4py import registry, utils

def register(name, file=None):
    '''
    Register the contents of the given file under the given name. If a file is not provided, use stdin.
    '''
    pkg, attr = registry.split_name(name)
    registry.initRegistry()
    if file:
        with open(file, 'r') as source_file:
            source = source_file.read()
    else:
        source = sys.stdin.read()
    try:
        obj = utils.loadIgnoreImports('temp', attr, source)
        if inspect.isroutine(obj):
            reg.register_function(pkg, attr, file)
        else:
            reg.register_pe(pkg, attr, file)
    except registry.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)        

def view(name):
    '''
    Display the source for the Dispel4Py entity identified by 'name'
    '''
    try:
        source = reg.get_code(name)
    except registry.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    if source is None:
        sys.stderr.write("Resource '%s' not found\n" % name)
    else:
        sys.stdout.write(source)
        
def list(name):
    '''
    List the contents of the package with 'name'.
    '''
    try:
        pkgs = reg.listPackages(name)
    except registry.UnknownPackageException as exc:
        sys.stderr.write("Unknown package: '%s'\n" % exc)
        sys.exit(3)
    except registry.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        print traceback.format_exc()
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    if pkgs: sys.stdout.write("Packages:\n")
    for pkg in pkgs:
        # ignore internal packages __gendef and __impl
        if not pkg.endswith('__gendef') and not pkg.endswith('__impl'):
            sys.stdout.write("   %s\n" % pkg)
    objs = []
    try:
        objs = reg.list(name)
    except registry.UnknownPackageException:
        pass
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    if objs: 
        sys.stdout.write("Processing Elements:\n\033[94m")
        for obj in objs:
            try:
                # write class PE or function
                if obj['type'] == 'eu.verce.registry.domains.PESig':
                    sys.stdout.write("  %s\n" % obj['name'])
            except:
                pass
        sys.stdout.write('\033[0m')
        sys.stdout.write("Functions:\n\033[92m")
        for obj in objs:
            try:
                if obj['type'] == 'eu.verce.registry.domains.FunctionSig':
                    sys.stdout.write("  %s\n" % obj['name'])
            except:
                pass
        sys.stdout.write('\033[0m')
        
def updateCode(name, code):
    ''' 
    Updates/replaces the source code of the given Dispel4Py component identified by 'name' 
    with the contents of 'code'.
    '''
    registry.initRegistry()
    reg.update_code(name, code)

def update(name, file):
    with open(file, "r") as src:
        code = src.read()
    updateCode(name, code)

def edit(name):
    '''
    Downloads the source code of the given Dispel4Py component identified by 'name' and opens
    an editor. When the editor is closed the modified source code is uploaded to the registry.
    '''
    temp_path = None
    try:
        if not def_editor:
            sys.stderr.write("Please specify environment variable EDITOR or 'default.editor' in the Dispel4Py configuration.")
        source = reg.get_code(name)
        fd, temp_path = tempfile.mkstemp()
        if source is not None:
            file = open(temp_path, 'w')
            data = file.write(source)
            file.close()
        os.close(fd)
        subprocess.call([def_editor, temp_path])
        if source is None:
            register(name, temp_path)
        else:
            update(name, temp_path)
    except Exception as err:
        if temp_path: os.remove(temp_path)
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    
def usage():
    sys.stderr.write("Usage: dispel4py <command> <arguments ...> \n")
    sys.stderr.write("Commands:\n")
    sys.stderr.write("  - list <package>: Lists all PEs and functions in the given package.\n")
    sys.stderr.write("  - register <name> <file>: Registers a dispel4py component specified in the given file, under the given name.\n")
    sys.stderr.write("  - view <name>: Displays the source code of the given Dispel4Py component.\n")
    sys.stderr.write("  - edit <name>: Edits the given dispel4py component and registers the modified source.\n")
    sys.stderr.write("  - update <name> <file>: Updates an existing Dispel4Py component.\n")

reg = registry.VerceRegistry()

def main():
   try:
       command = sys.argv[1]
   except IndexError:
       usage()
       sys.exit(1)

   configName = '.dispel4py/config.json'
   try:
       # look for an environment variable
       CONFIG = os.environ['DISPEL4PY_CONFIG'].encode('utf-8')
   except KeyError:
       if os.path.isfile(configName):
           # or is there a local file
           CONFIG = os.path.abspath(configName)
       else:
           # or in the user home directory
           CONFIG = os.path.expanduser('~/%s' % configName)

   try:
       with open(CONFIG, 'r') as config_file:
           conf = json.load(config_file)
   except:
       sys.stderr.write("No configuration found. Please ensure that the configuration is available at ~/%s or define $DISPEL4PY_CONFIG.\n" % configName)
       sys.exit(1) 

   reg_conf = conf['verce.registry']    
   reg.registry_url = reg_conf['url']
   reg.user = reg_conf['user']
   # reg.group = reg_conf['group']
   reg.workspace = reg_conf['workspace']

   if 'DISPEL4PY_CONFIG' in os.environ:
       def_editor = os.environ['EDITOR'].encode('utf-8')
   elif 'default.editor' in conf:
       def_editor = conf['default.editor']
   else:
       def_editor = None
        
   try:
       globals()[command](*sys.argv[2:])
   except KeyError:
       sys.stderr.write("Unknown command: %s\n" % command)
       usage()
       sys.exit(2)



if __name__ == '__main__':
   main()
   
