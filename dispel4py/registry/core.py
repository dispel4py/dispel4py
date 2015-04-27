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
# import traceback
import json
import os
import datetime
from tempfile import NamedTemporaryFile
# from dispel4py.registry import utils

import logging
logging.basicConfig()
logger = logging.getLogger('DJREG_LIB')
logger.setLevel(logging.INFO)


class Registry(object):
    """
    Dispel4Py's interface to the VERCE Registry. Dispel4Py could work without a
    registry or through connecting to alternative registries of python and
    dispel4py components. In this instance this makes use of the standard
    reference dispel4py Registry built as part of the EU VERCE project.
    """

    token_filename_prefix = 'djvercereg_token_'
    token_file = None
    token = None
    auth_header = ''  # TODO: Fill in / Internet connection...
    protocol = 'http'
    host = 'localhost'
    port = '8000'
    logged_in = False
    logged_in_time = None
    logged_in_username = None

    PASSWORD_EXPIRATION_PERIOD_HRS = 3

    DEF_WORKSPACE_ID = 1

    # REST service URL suffixes
    URL_AUTH = '/api-token-auth/'
    URL_USERS = '/users/'
    URL_REGISTRYUSERGROUPS = '/registryusergroups/'
    URL_GROUPS = '/groups/'
    URL_WORKSPACES = '/workspaces/'
    URL_PES = '/pes/'
    URL_FNS = '/functions/'
    URL_LITS = '/literals/'
    URL_CONNS = '/connections/'
    URL_PEIMPLS = '/pe_implementations/'
    URL_FNIMPLS = '/fn_implementations/'

    # Default package names depending on the type of the registrable item
    DEF_PKG_PES = 'pes'
    DEF_PKG_FNS = 'fns'
    DEF_PKG_LIT = 'lits'
    DEF_PKG_FNIMPLS = 'fnimpls'
    DEF_PKG_PEIMPLS = 'peimpls'
    DEF_PKG_WORKSPACES = 'workspaces'

    # JSON property names
    PROP_PEIMPLS = 'peimpls'
    PROP_FNIMPLS = 'fnimpls'

    # For resolving json object types
    TYPE_PE = 0
    TYPE_FN = 1
    TYPE_PEIMPL = 2
    TYPE_FNIMPL = 3
    TYPE_NOT_RECOGNISED = 100

    # Connection types
    CONN_TYPE_IN = 'IN'
    CONN_TYPE_OUT = 'OUT'

    # OLD #######################
    # registry_url = DEF_URL
    workspace = DEF_WORKSPACE_ID
    # user = None
    registered_entities = {}
    # token = None
    # ###########################

    def __init__(self, wspc_id=DEF_WORKSPACE_ID):
        # this imports the requests module before anything else
        # so we don't get a loop when importing
        requests.get('http://github.com')
        # change the registry URL according to the environment var, if set
        if 'VERCEREGISTRY_HOST' in os.environ:
            self.protocol, self.host, self.port = split_url(
                os.environ['VERCEREGISTRY_HOST'])
            # self.registry_url = os.environ['VERCEREGISTRY_HOST']

        self.workspace = wspc_id

    def get_auth_token(self):
        if not self.logged_in:
            raise NotLoggedInError()
            return
        with open(self.token_file, 'r') as f:
            token = f.readline().strip()
        return token

    def _get_auth_header(self):
        """
        Return the authentication header as used for requests to the
        registry.
        """
        return {'Authorization': 'Token %s' % (self.get_auth_token())}

    def _valid_login(self, username):
        """
        Return true if the client has already logged in and login is valid.
        """
        return (self.logged_in and self.logged_in_username == username
                and datetime.datetime.now() -
                self.logged_in_time.total_seconds() <
                self.PASSWORD_EXPIRATION_PERIOD_HRS * 60 * 60)

    def _extract_kind_from_json_object(self, j):
        """
        The kind/type is inferred based on the URL. Assumes this is called for
        single objects.
        Return one of the TYPE* values defined in VerceRegManager.
        """
        # logger.info(j)
        rhs = j['url'][len(self.get_base_url())+1:]
        kind = rhs[:rhs.find('/')]

        if kind == 'pes':
            return Registry.TYPE_PE
        elif kind == 'functions':
            return Registry.TYPE_FN
        else:
            return Registry.TYPE_NOT_RECOGNISED

    def login(self, username, password):
        """
        (Lazily) log into vercereg with the provided credentials.

        :param username
        :param password
        """
        if self._valid_login(username):
            return True

        data = {'username': username, 'password': password}
        url = self.get_base_url() + self.URL_AUTH

        r = requests.post(url, data, verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            if r.status_code == requests.codes.forbidden:
                raise NotAuthorisedException()
            else:
                raise
        self.logged_in = r.status_code == 200
        if self.logged_in:
            self.logged_in_time = datetime.datetime.now()
            self.logged_in_username = username
            f = NamedTemporaryFile(prefix=self.token_filename_prefix,
                                   delete=False)
            self.token_file = f.name
            self.token = json.loads(r.text)['token']
            f.write(self.token)
            f.close()
        return self.logged_in

    def get_base_url(self):
        """Return the base URL for the registry."""
        return self.protocol + '://' + self.host + (self.port and ':' +
                                                    self.port or '')

    def set_workspace(self, wspc_id):
        self.workspace = wspc_id

    def find_module(self, fullname, path=None):
        print '---- (find_module) fullname=' + str(fullname),
        'path=' + str(path)
        # try:
        url = (self.get_base_url() +
               "/workspaces/%s?ls&startswith=%s" % (self.workspace, fullname))
        response = requests.get(url, headers=self._get_auth_header(),
                                verify=False)
        # except:
        #     pass
        if response.status_code != requests.codes.ok:
            return None

        # maybe it's a package
        if len(response.json()['packages']) > 0:
            return self

        # maybe it's an object?
        try:
            code = self.get_code(fullname)
            # print "found code for " + fullname
            self.registered_entities[fullname] = code
            return self
        except:
            return None

    def clone(self, orig_workspace_id, name):
        """Clone the given workspace into a new one with the name `name`. """
        if not self.logged_in:
            raise NotLoggedInError()
            return

        url = (self.get_base_url() + self.URL_WORKSPACES + '?clone_of=' +
               str(orig_workspace_id))
        r = requests.post(url, data={'name': name},
                          headers=self._get_auth_header(), verify=False)
        print r.text

    def load_module(self, fullname):
        print ">>>> load_module " + fullname
        if fullname in sys.modules:
            return sys.modules[fullname]

        mod = imp.new_module(fullname)
        mod.__loader__ = self
        sys.modules[fullname] = mod
        if fullname in self.registered_entities:
            code = self.registered_entities[fullname]
            # print "compiling code for module " + fullname
            exec code in mod.__dict__
        mod.__file__ = "[%r]" % fullname
        mod.__path__ = []
        return mod

    def get_code(self, fullname, workspace_id=DEF_WORKSPACE_ID):
        """
        First try pe implementations and then fns. If all fails raise an
        ImplementationNotFound exception.
        """
        toks = fullname.split('.')
        pckg = toks[0]
        name = toks[1]

        try:
            return self.get_pe_implementation_code(workspace_id, pckg, name)
        except:
            pass

        try:
            return self.get_fn_implementation_code(workspace_id, pckg, name)
        except:
            pass

        # if all else fails, attempt to fetch the direct implementation
        return self.get_direct_implementation_code(workspace_id, pckg, name)

    def get_pe_implementation_code(self, workspace_id, pckg, name,
                                   impl_index=0):
        """
        Return the implementation code of the given PE / identified by
        pckg-name.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace_id) +
               '/?fqn=' + pckg + '.' + name)
        r = requests.get(url, headers=self._get_auth_header(), verify=False)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        if self.PROP_PEIMPLS not in r.json():
            raise NotPEError()

        try:
            impl_url = r.json()[self.PROP_PEIMPLS][impl_index]
        except IndexError:
            raise ImplementationNotFound()
            return

        r = requests.get(impl_url, headers=self._get_auth_header(),
                         verify=False)
        return r.json().get('code')

    def get_direct_implementation_code(self, workspace_id, pckg, name):
        """
        Return the implementation code for the given PEImplementation (not its
        specification parent).
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace_id) +
               '/?fqn=' + pckg + '.' + name)
        r = requests.get(url, headers=self._get_auth_header(), verify=False)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        print r
        try:
            return r.json().get('code')
        except:
            raise ImplementationNotFound()

    def get_fn_implementation_code(self, workspace_id, pckg, name,
                                   impl_index=0):
        """
        Return the implementation code of the given function / identified
        by pckg-name.
        """
        if not self.logged_in:
            raise NotLoggedInError()
            return

        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace_id) +
               '/?fqn=' + pckg + '.' + name)
        r = requests.get(url, headers=self._get_auth_header(), verify=False)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()
            return

        if self.PROP_FNIMPLS not in r.json():
            raise NotFunctionError()
            return

        try:
            impl_url = r.json()[self.PROP_FNIMPLS][impl_index]
        except IndexError:
            raise ImplementationNotFound()
            return

        r = requests.get(impl_url, headers=self._get_auth_header(),
                         verify=False)
        return r.json().get('code')

##############################################################################
# Utility and static methods:
##############################################################################


def remove_registry_from_meta_path():
    mylist = [i for i in sys.meta_path if type(i) != Registry]
    sys.meta_path = mylist


def currentRegistry():
    """
    Returns the currently used registry instance.
    """
    for i in sys.meta_path:
        if isinstance(i, Registry):
            return i

DEF_WORKSPACE_ID = 1


def initRegistry(username=None, password=None, url=None,
                 workspace=DEF_WORKSPACE_ID, token=None):
    """
    Initialises the registry. This method must be called before any 'import'
    statements.
    """
    remove_registry_from_meta_path()
    reg = Registry()
    reg.workspace = workspace
    if url:
        reg.protocol, reg.host, reg.port = split_url(url)
    print reg.protocol, reg.host, reg.port
    reg.user = username
    if token:
        reg.token = token
        response = requests.get(url + 'dummy', headers=reg._get_auth_header())
        if response.status_code == requests.codes.forbidden:
            raise NotAuthorisedException()
        else:
            response.raise_for_status()
    else:
        reg.login(username, password)
    sys.meta_path.append(reg)
    return reg


def split_name(fullname):
    """
    Split a pckg.name string into its package and name parts.

    :param fullname: an entity name in the form of <package>.<name>
    """
    parts = fullname.split('.')
    pkg = ".".join(parts[:-1])
    simpleName = parts[-1]
    return pkg, simpleName


def split_url(url):
    """
    Splits a string url and returns a (protocol,host,port) tuple.
    It assumes the format protocol://host:port,
    e.g.: http://escience2.inf.ed.ac.uk/registry:8080

    :param url: A string denoting a URL in the standard format.
    """
    toks = url.split(':')
    try:
        return toks[0], toks[1][2:], toks[2]
    except IndexError:
        return toks[0], toks[1][2:], None


class VerceRegClientLibError(Exception):
    pass


class ImplementationNotFound(Exception):
    pass


class NotPEError(Exception):
    pass


class NotFunctionError(Exception):
    pass


class NotAuthorisedException(VerceRegClientLibError):
    pass


class UnknownPackageException(VerceRegClientLibError):
    pass


class RegistrationFailed(VerceRegClientLibError):
    pass


class NotLoggedInError(VerceRegClientLibError):
    def __init__(self, msg='Log in required; please log in first.'):
        self.msg = msg


def createResources(resources_dir, registry):
    """
    Caches source code imported from the registry

    :param resources_dir: directory for caching the source code
    :param registry: the dispel4py registry, may be None.
    """
    if not registry:
        return
    for mod, code in registry.registered_entities.iteritems():
        store_resource(resources_dir, mod, code)


def store_resource(resources_dir, mod, code):
    """
    Stores the source of the given python module to a file.

    :param resources_dir: directory to store the source
    :param mod: module name
    :param code: source code
    """
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


def main():
    print 'Main for testing starting'
    reg = initRegistry(username='iraklis', password='iraklis')
    print reg.find_module(fullname='pes.RandomWordProducer')

if __name__ == '__main__':
    main()
