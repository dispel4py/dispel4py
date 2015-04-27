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

import argparse
import sys
import os
import json
import base64
import inspect
import tempfile
import subprocess
import traceback
import getpass

from dispel4py.registry import utils
from dispel4py.registry import core

DISPEL4PY_CONFIG_DIR = os.path.expanduser('~/.dispel4py/')
DISPEL4PY_CONFIG_DIR = os.path.expanduser('.dispel4py/')
CACHE = DISPEL4PY_CONFIG_DIR + '.cache'
CONFIG_NAME = 'config.json'

def login(username, password=None, conf=None):
    if username is None:
        username = raw_input('Username: ') 
    if password is None:
        password = getpass.getpass('Password: ')
    if conf is None:
        conf=configure()
    workspace = conf['verce.registry']['workspace']
    url = None
    if 'VERCEREGISTRY_HOST' in os.environ:
        url = os.environ['VERCEREGISTRY_HOST'] + '/rest/'
    else:
        url = conf['verce.registry']['url']
    try:
        reg = core.initRegistry(username, password, url=url, workspace=workspace)
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.\n")
        sys.exit(4)
    enc = base64.b64encode(password)
    try:
        os.mkdir(DISPEL4PY_CONFIG_DIR)
    except:
        pass
    with open(CACHE, 'w') as file:
        file.write('%s\n%s\n%s' % (username, enc, reg.token))
    # print 'Logged in.'
    return reg
        
def removeCache():
    try:
        os.remove(CACHE)
    except:
        pass
    print 'Cleared login cache.'
        
def _initRegistry(config=None, username=None, password=None):
    token = None
    if (config==None):
        config = configure()
    if not username:
        try:
            with open(CACHE, 'r') as file:
                [username, enc, token] = file.read().splitlines()
                password = base64.b64decode(enc)
        except IOError:
            username = raw_input("Username: ")  
            password = getpass.getpass('Password: ')
    elif not password:
        password = getpass.getpass('Password: ')

    reg_conf = config['verce.registry']   
    try:
        workspace = reg_conf['workspace']
    except KeyError:
        sys.stderr.write('Must specify workspace for registry.')
        sys.exit(-2)
    try:
        url = reg_conf['url']
    except KeyError:
        url = registry.DEF_URL
        
    try:
        if token:
            try:
                reg = core.initRegistry(username=username, token=token, url=url, workspace=workspace)
            except core.NotAuthorisedException:
                reg = login(username, password)
        else:
            reg = login(username, password)
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.\n")
        sys.exit(4)
    
    return reg

def register(reg, name, attr, file=None):
    '''
    Register the contents of the given file under the given name. If a file is not provided, use stdin.
    '''
    pkg, simpleName = core.split_name(name)
    if file:
        with open(file, 'r') as source_file:
            source = source_file.read()
    else:
        source = sys.stdin.read()
    try:
        obj = utils.loadIgnoreImports('temp', attr, source)
        if inspect.isroutine(obj):
            reg.register_function(name, attr, file)
        else:
            reg.register_pe(name, attr, file)
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.\n")
        sys.exit(4)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)

def __select_wspc(reg):
    ''' List all workspaces and allow the user to select one - return (workspace's id, name) '''
    
    resp = None
    try:
        resp = reg.listWorkspaces()
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    count = 0
    for i in resp.json():
        print '(' + str(count + 1) + ') ' + i['name']
        count += 1
    choice = None
    try:
        choice = int(raw_input('Your selection: '))
    except ValueError:
        sys.stderr.write('Invalid selection. Please enter a valid number.\n')
        sys.exit(1)
    except:
        sys.stderr.write('Unknown error occurred.\n')
        sys.exit(2)
    if choice == 0:
        print 'Exiting...'
        sys.exit(0)
    if choice < 0 or choice > len(resp.json()):
        sys.stderr.write('Invalid selection - out of bounds.\n')
        sys.exit(3)
    
    return (resp.json()[choice-1]['id'],resp.json()[choice-1]['name'])

def clone_workspace(reg):
    ''' Clone a workspace by choosing one of the existing ones '''
    
    print 'Please select a workspace to clone by providing the corresponding number. Enter 0 to exit.'
    wspc_id, wspc_name = __select_wspc(reg)
    
    new_name = raw_input('Please provide a name for the new workspace: ')
    try:
        reg.clone(wspc_id, new_name)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    
    print 'New workspace created \'' + new_name + '\' as a clone of \'' + wspc_name + '\''

def workspace(reg):
    ''' Choose a new default workspace '''

    print 'Please select one of the following workspaces by providing the corresponding number. Enter 0 to exit.'
    wspc_id, wspc_name = __select_wspc(reg)
    set_default_workspace(wspc_id)
    print 'Active workspace changed to \'' + wspc_name + '\'' 
    
    
def view(reg, name):
    '''
    Display the source for the Dispel4Py entity identified by 'name'
    '''
    try:
        source = reg.get_code(name)
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    if source is None:
        sys.stderr.write("Resource '%s' not found\n" % name)
    else:
        sys.stdout.write(source)
        sys.stdout.write('\n')

def __lst_pckgs(reg):
    '''List the packages of a workspace'''
    packages = set([])
    wspc_json = None
    try:
        wspc_json = reg.listPackages('')
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        print traceback.format_exc()
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
    
    if wspc_json == []:
        sys.stderr.write('Empty response received. The default workspace may not exist. Try switching to a valid workspace by calling "workspace" first.\n')
        sys.exit(5)
        
    for wi in wspc_json['workspaceItems']:
        p = wi['pckg']
        if not p.endswith('__gendef') and not p.endswith('__impl'):
            packages.add(str(wi['pckg']))
    # packs =  list(packages) -- this causes an error for whatever reason...
    packs=[]
    for i in packages:
        packs.append(i)
    packs.sort()
    if len(packs)>0:
        print 'Packages:'
        print '\n'.join('   ' + p for p in packs)
    else:
        print 'No packages found.'
        
def list(reg, name=''):
    '''
    List the contents of the package with 'name'.
    '''
    
    print 'Listing of Workspace:' + str(reg.workspace)
    # XXX: Refactor / special case for name=''
    if name=='':
        __lst_pckgs(reg)
        return
        
    try:
        pkgs = reg.listPackages(name)
    except core.UnknownPackageException as exc:
        sys.stderr.write("Unknown package prefix: '%s'\n" % exc)
        sys.exit(3)
    except core.NotAuthorisedException:
        sys.stderr.write("Not authorised.")
        sys.exit(4)
    except Exception as err:
        print traceback.format_exc()
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)
        
    if pkgs == []:
        sys.stderr.write('Empty response received. The default workspace may not exist. Try switching to a valid workspace by calling "workspace" first.\n')
        sys.exit(5)
        
    if pkgs: sys.stdout.write("Packages:\n")
    for pkg in pkgs:
        # ignore internal packages __gendef and __impl
        if not pkg.endswith('__gendef') and not pkg.endswith('__impl'):
            sys.stdout.write("   %s\n" % pkg)
    objs = []
    try:
        objs = reg.list(name)
    except core.UnknownPackageException:
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
        
def delete(reg, name):
    reg.delete(name)
        
def updateCode(reg, name, code):
    ''' 
    Updates/replaces the source code of the given Dispel4Py component identified by 'name' 
    with the contents of 'code'.
    '''
    reg.update_code(name, code)

def update(reg, name, file):
    with open(file, "r") as src:
        code = src.read()
    updateCode(reg, name, code)

def edit(reg, name):
    '''
    Downloads the source code of the given Dispel4Py component identified by 'name' and opens
    an editor. When the editor is closed the modified source code is uploaded to the registry.
    '''
    temp_path = None
    def_editor = getEditor()
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
            update(reg, name, temp_path)
    except Exception as err:
        if temp_path: os.remove(temp_path)
        import traceback
        traceback.print_exc()
        sys.stderr.write("An error occurred:\n%s\n" % err)
        sys.exit(-1)

def usage():
    sys.stderr.write("Usage: dispel4py <command> <arguments ...> \n")
    sys.stderr.write("Commands:\n")
    sys.stderr.write("  - login [-u USERNAME] [-p PASSWORD]: Log in and store the username and password.")
    sys.stderr.write("  - list <package>: Lists all PEs and functions in the given package.\n")
    sys.stderr.write("  - register <name> <file>: Registers a dispel4py component specified in the given file, under the given name.\n")
    sys.stderr.write("  - view <name>: Displays the source code of the given Dispel4Py component.\n")
    sys.stderr.write("  - edit <name>: Edits the given dispel4py component and registers the modified source.\n")
    sys.stderr.write("  - update <name> <file>: Updates an existing Dispel4Py component.\n")

def getEditor():
    def_editor = None
    try:
        def_editor = os.environ['EDITOR'].encode('utf-8')
    except KeyError:
        pass
    try:
        def_editor = config['default.editor']
    except:
        pass
    return def_editor

def configure():
    configName = '.dispel4py/' + CONFIG_NAME
    try:
        # look for an environment variable
        CONFIG = os.environ['DISPEL4PY_CONFIG'].encode('utf-8')
    except KeyError:
        if os.path.isfile(configName):
            # or is there a local file
            CONFIG = os.path.abspath(configName)
        else:
            # or in the user home directory
            CONFIG = os.path.abspath('%s%s' % (DISPEL4PY_CONFIG_DIR, CONFIG_NAME))
            CONFIG = os.path.abspath(DISPEL4PY_CONFIG_DIR + CONFIG_NAME)
    if not os.path.isfile(CONFIG):
        # create config
        conf = { 'verce.registry' : { 'url' : core.DEF_URL, 'workspace' : core.DEF_WORKSPACE } }
        with open(CONFIG, 'w') as config_file:
            config_file.write(json.dumps(conf))
    else:
        try:
            with open(CONFIG, 'r') as config_file:
                conf = json.load(config_file)
        except:
            # the file exists but it's invalid - remove it so that it gets recreated next time idispel4py is run
            os.remove(configName)
            sys.stderr.write("No valid configuration found. Please ensure that the configuration is available at ~/%s or define $DISPEL4PY_CONFIG.\n" % configName)
            sys.exit(1)
    return conf
    
    
def set_default_workspace(wspc_id):
    CONFIG = None 
    if 'DISPEL4PY_CONFIG' in os.environ:
        CONFIG = os.environ['DISPEL4PY_CONFIG'].encode('utf-8')
    else:
        CONFIG = './.dispel4py/' + CONFIG_NAME
    conf = configure()
    conf['verce.registry']['workspace'] = wspc_id

    with open(CONFIG, 'w') as config_file:
        config_file.write(json.dumps(conf))
    print 'Updated ' + CONFIG


def main():
    parser = argparse.ArgumentParser(description='View and register Dispel4Py objects in a registry.')
    parser.add_argument('command', help='command to execute, one of: list, view, register, workspace')
    parser.add_argument('args', nargs='*', help='command arguments')
    parser.add_argument('-u', '--username', help='username for registry access')
    parser.add_argument('-p', '--password', help='password')
    args = parser.parse_args()
    
    if args.command == 'login':
        login(args.username, args.password)
    elif args.command == 'exit':
        removeCache()
    else:
        config = configure()
        reg = _initRegistry(config, args.username, args.password)
        try:
            globals()[args.command](reg, *args.args)
        except KeyError:
            sys.stderr.write("Unknown command: %s\n" % args.command)
            usage()
    
if __name__ == '__main__':
    main()
