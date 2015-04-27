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

import imp
import sys
import requests
import traceback
import json
import os
from dispel4py.registry import utils

DEF_URL = 'http://escience8.inf.ed.ac.uk:8080/VerceRegistry/rest/'
DEF_WORKSPACE = 1
PKG_IMPLEMENTATION = ".__impl"
PKG_GENERICDEF = ".__gendef"
AUTH_HEADER = 'X-Auth-Token'

class VerceRegistry(object):
    '''
    Dispel4Py's interface to the VERCE Registry. Dispel4Py could work withut a registry or through 
    connecting to alternative registries of python and dispel4py components. In this instance this 
    makes use of the VERCE Registry's REST API.
    '''
    
    registry_url = DEF_URL
    workspace = DEF_WORKSPACE
    user = None
    registered_entities = {}
    token = None
    
    def __init__(self, wspc_id=DEF_WORKSPACE):
        # this imports the requests module before anything else
        # so we don't get a loop when importing
        requests.get('http://github.com')
        # change the registry URL according to the environment var, if set
        if 'VERCEREGISTRY_HOST' in os.environ:
            self.registry_url = os.environ['VERCEREGISTRY_HOST']
        
        self.workspace = wspc_id
        
        # print 'Initialised VerceRegistry object for ' + self.registry_url
        
    def set_workspace(self, wspc_id):
        self.workspace = wspc_id
    
    def login(self, user, password):
        url = self.registry_url + 'login?username=%s&password=%s' % (user, password)
        response = requests.post(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if response.status_code == requests.codes.forbidden:
                raise NotAuthorisedException()
            else:
                raise
        try:
            self.token = response.json()['access_token']
            self.user = user
        except:
            raise NotAuthorisedException()
            
    def find_module(self, fullname, path=None):
        try:
            url = self.registry_url + "workspace/%s?packagesByPrefix=%s&exists=true" % (self.workspace, fullname)
            response = requests.get(url, headers=getHeaders(self.token))
        except:
            return None
        if response.status_code != requests.codes.ok:
            return None
        if response.json()[0]:
            return self
        else:
            # maybe it's an object?
            code = self.get_code(fullname)
            if code is None:
                return None
            else:
                # print "found code for " + fullname
                self.registered_entities[fullname] = code
                return self

    def clone(self, wspc_id, clone_name):
        ''' Clone the given Workspace(wspc_id) into a new one named clone_name. '''
        try:
            url = self.registry_url + 'workspace/%s?cloneTo=%s' % (wspc_id, clone_name)
            data = { 'workspace': { 'id': wspc_id } }
            # print url
            # print json.dumps(data)
            response = requests.post(url, headers=getHeaders(self.token), data=json.dumps(data))
            # print response.json()
        except Exception as err:
            print traceback.format_exc()
            sys.stderr.write("An error occurred:\n%s\n" % err)
            sys.exit(-1)

    def load_module(self, fullname):
        # print "load_module " + fullname
        if fullname in sys.modules:
            return sys.modules[fullname]
            
        mod = imp.new_module(fullname)
        mod.__loader__ = self
        sys.modules[fullname] = mod
        if fullname in self.registered_entities:
            code = self.registered_entities[fullname]
            print "compiling code for module " + fullname
            exec code in mod.__dict__
        mod.__file__ = "[%r]" % fullname
        mod.__path__ = []
        return mod

    def get_code(self, fullname):
        '''
        Retrieves and returns the source code of the dispel4py component identified by 'fullname'. 
        'fullname' is in the form package.name. 
        '''
        impl_id = self.getImplementationId(fullname)
        if impl_id:
            response = requests.get(self.registry_url + "implementation/%s" % impl_id, headers=getHeaders(self.token))
            if response.status_code == requests.codes.ok:
                return response.json()["code"]
                
    def update_code(self, fullname, code):
        ''' 
        Updates/replaces the source code of the given dispel4py component identified by 'fullname' 
        with the contents of 'code'.
        '''
        impl_id = self.getImplementationId(fullname)
        impl = {}
        impl["user"] = { 'username' : self.user }
        impl["workspace"] = self.workspace
        impl["code"] = code
        data = { 'implementation' : impl }
        url = self.registry_url + "implementation/%s" % impl_id
        response = requests.put(url, headers=getHeaders(self.token), data=json.dumps(data))
        if response.status_code != requests.codes.ok:
            raise Exception("Implementation update failed")
        response_json = response.json()
        if "errors" in response_json:
            print "Error: %s" % response_json["errors"]
            raise Exception("Implementation update failed")
        return response_json["id"]

    def getImplementationId(self, fullname):
        pkg, simpleName = split_name(fullname)
        url = self.registry_url + "workspace/%s/%s/%s?deep=true" % (self.workspace, pkg, simpleName)
        try:
            response = requests.get(url, headers=getHeaders(self.token))
        except:
            return None

        if response.status_code != requests.codes.ok:
            return None
        
        json = response.json()
        if "implementations" in json:
            impl_id = json["implementations"][0]["id"]
            return impl_id
                
    def register_gendef(self, pkg, simpleName):
        gendef = {}
        gendef["user"] = { 'username' : self.user }
        gendef["workspaceId"] = self.workspace
        gendef["pckg"] = pkg + PKG_GENERICDEF
        gendef["name"] = simpleName
        data = {}
        data["gendef"] = gendef;
        response = requests.post(self.registry_url + "gendef/", data=json.dumps(data), headers=getHeaders(self.token))
        try:
            response.raise_for_status()
        except:
            print response.text
            raise RegistrationFailed, "Registration of generic definition failed", sys.exc_info()[2]
        return response.json().get("id")
                
    def register_implementation(self, sigId, pkg, simpleName, path):
        with open(path, "r") as src:
            code = src.read()
        impl = {}
        impl["user"] = { 'username' : self.user }
        impl["workspaceId"] = self.workspace
        impl["pckg"] = pkg + PKG_IMPLEMENTATION
        impl["name"] = simpleName
        impl["genericSigId"] = sigId
        impl["code"] = code
        data = {}
        data["implementation"] = impl
        response = requests.post(self.registry_url + "implementation/", data=json.dumps(data), headers=getHeaders(self.token))
        try:
            response.raise_for_status()
        except:
            raise RegistrationFailed, "Registration of implementation failed", sys.exc_info()[2]
        response_json = response.json()
        if "errors" in response_json:
            print "Error: %s" % response_json["errors"]
            raise Exception("Registration of implementation failed")
        return response_json["id"]
        
    def register_function(self, fullname, functionName, path):
        '''
        Registers a dispel4py/python function with the VERCE Registry. The function is registered under 
        'fullname' and it is identified by 'functionName'. 'path' is the path to a file containing
        the source code of the function to be registered.
        '''           
        pkg, simpleName = split_name(fullname)
        
        # load the code
        funImpl = utils.loadSource(simpleName, path, functionName)
        funAnn = utils.extractAnnotations(funImpl)
        
        # build the function signature
        function = {}
        function["user"] = { 'username' : self.user }
        function["workspaceId"] = self.workspace
        function["pckg"] = pkg
        function["name"] = simpleName
        function["parameters"]=[]
        for param in funAnn['params']:
            function["parameters"].append(param['type'] + " " + param['name'])
        function["returnType"]=funAnn['return']
        data = {}
        data["function"] = function
        
        # print "Registering function " + simpleName + " in " + pkg
        genDefId = self.register_gendef(pkg, simpleName)
        # print "Registered generic definition: id = %s" % genDefId
        
        # register function signature
        function["genericDefId"] = genDefId
        try:
            response = requests.post(self.registry_url + "function/", data=json.dumps(data), headers=getHeaders(self.token))
            try:
                response.raise_for_status()
            except:
                requests.delete(self.registry_url + "gendef/%s" % genDefId)
                raise RegistrationFailed, "Registration of function signature failed", sys.exc_info()[2]
            functionId = response.json()["id"]
            # print "Registered function signature: id = %s" % functionId
            implId = self.register_implementation(functionId, pkg, simpleName, path)
            # print "Registered implementation:     id = %s" % implId
        except:
            requests.delete(self.registry_url + "gendef/%s" % genDefId)
            raise
            
    def register_pe(self, fullname, className, path):   
        '''
        Registers a dispel4py processing element (PE) with the VERCE Registry. The PE is registered under 
        'fullname' and it is identified by 'className'. 'path' is the path to a file containing
        the source code of the PE to be registered.
        '''          
        pkg, simpleName = split_name(fullname)
        
        # load the code
        peImpl = utils.loadSource(simpleName, path, className)()
        
        # prepare the PE signature
        peSig = {}
        peSig["user"] = { 'username' : self.user }
        peSig["workspaceId"] = self.workspace
        peSig["pckg"] = pkg
        peSig["name"] = simpleName
        connections = []
        for conx in peImpl.inputconnections.values():
            connection = {}
            connection["name"] = conx['name']
            connection["kind"] = 0
            connection["modifiers"] = []
            connections.append(connection)
        for conx in peImpl.outputconnections.values():
            connection = {}
            connection["name"] = conx['name']
            connection["kind"] = 1
            connection["modifiers"] = []
            connections.append(connection)
        peSig["connections"] = connections
        data = {}
        data["pesig"] = peSig
        
        # Register generic signature
        # print "Registering PE " + simpleName + " in " + pkg
        genDefId = self.register_gendef(pkg, simpleName)
        # print "Registered generic definition: id = %s" % genDefId
        try:
            # Register PE signature
            peSig["genericDefId"] = genDefId
            response = requests.post(self.registry_url + "pe/", data=json.dumps(data), headers=getHeaders(self.token))
            try:
                response.raise_for_status()
            except:
                requests.delete(self.registry_url + "gendef/%s" % genDefId)
                raise RegistrationFailed, "Registration of PE signature failed", sys.exc_info()[2]
            peId = response.json()["id"]
            # print "Registered PE signature:   id = %s" % peId
            # Register implementation
            implId = self.register_implementation(peId, pkg, simpleName, path)
            # print "Registered implementation:     id = %s" % implId
        except:
            # delete everything that was registered if anything went wrong
            requests.delete(self.registry_url + "gendef/%s" % genDefId)
            raise
            
    def list(self, pkg):
        '''
        Lists the contents of package 'pkg'.
        '''
        url = self.registry_url + "workspace/%s/%s" % (self.workspace, pkg)
        response = requests.get(url, headers=getHeaders(self.token))
        result = []
        if response.status_code == requests.codes.ok:
            response_json = response.json()
            for obj in response_json:
                desc = { 'name' : obj['name'], 'type' : obj['class'] }
                result.append(desc)
        elif response.status_code == requests.codes.not_found: # not found
            raise UnknownPackageException(pkg)
        return result
            
    def listPackages(self, pkg):
        '''
        Lists the packages contained within package 'pkg'.
        '''
        url = self.registry_url + "workspace/%s?packagesByPrefix=%s" % (self.workspace, pkg)
        response = requests.get(url, headers=getHeaders(self.token))
        # print json.dumps(response.json(), sort_keys=True, indent=4)
        result = []
        if response.status_code == requests.codes.ok:
            result = response.json()
        elif response.status_code == requests.codes.not_found: # not found
            raise UnknownPackageException(pkg)
        return result
        
    def delete(self, fullname):
        pkg, simpleName = split_name(fullname)
        # assume that the gen def is defined in subpackage PKG_GENERICDEF
        url = self.registry_url + "workspace/%s/%s%s/%s" % (self.workspace, pkg, PKG_GENERICDEF, simpleName)
        response = requests.get(url, headers=getHeaders(self.token))
        if response.status_code == requests.codes.ok:
            genDefId = response.json()["id"]
            response = requests.delete(self.registry_url + "gendef/%s" % genDefId, headers=getHeaders(self.token))
            if response.status_code == requests.codes.ok:
                print "Deleted " + fullname
            else:
                print "Failed to delete %s" % fullname
                print response.text
        else:
            print "Cannot find " + fullname
            
    def createWorkspace(self, name):
        url = self.registry_url + 'workspace'
        data = { 'workspace': { 'name' : name, 'owner' : self.user } }
        response = requests.post(url, data=json.dumps(data), headers=getHeaders(self.token))
        if response.status_code == requests.codes.forbidden:
            raise NotAuthorisedException()
        if response.status_code != requests.codes.ok:
            raise RegistrationFailed()
    
    def listWorkspaces(self):
        url = self.registry_url + 'workspace'
        response = requests.get(url, headers=getHeaders(self.token))
        if response.status_code == requests.codes.forbidden:
            raise NotAuthorisedException()
        if response.status_code != requests.codes.ok:
            raise RegistrationFailed()
        return response

##############################################################################
# Utility and static methods: 
##############################################################################

def remove_registry_from_meta_path():
    mylist = [ i for i in sys.meta_path if type(i) != VerceRegistry ]
    sys.meta_path = mylist

def currentRegistry():
    '''
    Returns the currently used registry.
    '''
    for i in sys.meta_path:
        if isinstance(i, VerceRegistry): return i

def initRegistry(username=None, password=None, url=DEF_URL, workspace=DEF_WORKSPACE, token=None):
    '''
    Initialises the registry. This method must be called before any 'import' statements.
    '''
    remove_registry_from_meta_path()
    reg = VerceRegistry()
    reg.workspace = workspace
    reg.registry_url = url
    reg.user = username
    
    # FIXME Check 'dummy' works on the registry and if not, add it - this is to quickly see if we're logged in
    if token: 
        reg.token = token
        response = requests.get(url + 'dummy', headers=getHeaders(token))
        if response.status_code == requests.codes.forbidden:
            raise NotAuthorisedException()
        else:
            response.raise_for_status()
    else:
        reg.login(username, password)
    
    sys.meta_path.append(reg)
    return reg
        
def split_name(fullname):
    parts = fullname.split('.')
    pkg = ".".join(parts[:-1])
    simpleName = parts[-1]
    return pkg, simpleName    
    
def getHeaders(token):
    if token:
        return { AUTH_HEADER : token }
    else:
        raise NotAuthorisedException()
    
class NotAuthorisedException(Exception):
    pass    

class UnknownPackageException(Exception):
    pass
    
class RegistrationFailed(Exception):
    pass

import os

def createResources(resources_dir, registry):
    ''' 
    Caches source code imported from the registry 
    
    :param resources_dir: directory for caching the source code
    :param registry: the dispel4py registry, may be None.
    '''    
    if not registry:
        return
    for mod, code in registry.registered_entities.iteritems():
        store_resource(resources_dir, mod, code)

def store_resource(resources_dir, mod, code):
    '''
    Stores the source of the given python module to a file.
    
    :param resources_dir: directory to store the source
    :param mod: module name
    :param code: source code
    '''
    try:
        pkg = mod.replace(".", "/")
        path = "%s/%s.py" % (resources_dir, pkg)
        dir = os.path.dirname(path)
        if not os.path.exists(dir):
            os.makedirs(dir)
            fullpath = resources_dir + '/'
            for part in pkg.split('/')[:-1]:
                fullpath += part + '/'
                open(fullpath + "__init__.py", 'w').close() 
        with open(path, "w") as code_file:
            code_file.write(code)
        # print "Wrote source code to %s" % path 
    except AttributeError:
        print "Warning: Could not find source code for module " + mod
    except Exception as exc:
        print exc
        print "Warning: Could not store source code for module " + mod
