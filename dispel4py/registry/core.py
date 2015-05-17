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

# import imp
import sys
import requests
requests.packages.urllib3.disable_warnings()

# import traceback
import json
import os
import datetime
from tempfile import NamedTemporaryFile
# from dispel4py.registry import utils
from os.path import expanduser

import logging
logging.basicConfig()
logger = logging.getLogger('DJREG_LIB')
logger.setLevel(logging.INFO)

from urlparse import urlparse


class RegistryConfiguration(object):
    """An encapsulation of the configuration of the interface with the
    registry."""
    url = None
    username = None
    password = None
    def_workspace = None

    DEF_CONF_FILENAME = '.d4p_reg.json'
    conf_file = None

    # Environment variable constants:
    CONF_VAR = 'D4P_REG_CONF'
    URL_VAR = 'D4P_REG_URL'
    USERNAME_VAR = 'D4P_REG_USERNAME'
    PASSWORD_VAR = 'D4P_REG_PASSWORD'
    WORKSPACE_VAR = 'D4P_REG_WORKSPACE'

    # Configuration file constants:
    URL_FILE = 'URL'
    USERNAME_FILE = 'username'
    PASSWORD_FILE = 'password'
    WORKSPACE_FILE = 'workspace'

    def __init__(self):
        self.conf_file = expanduser('~/' +
                                    RegistryConfiguration.DEF_CONF_FILENAME)
        try:
            self.conf_file = expanduser(
                os.environ[RegistryConfiguration.CONF_VAR])
        except:
            pass
        self.loadFromEnvVars()
        self.loadFromFile()

        # Check we have the required configuration values:
        if (self.url is None or
                self.username is None or
                self.password is None or
                not self._valid_url()):
            msg = 'A valid URL, username and password were not provided.'
            msg += '\nPlease refer to documentation on setting up the '
            msg += 'dispel4py Registry environment.'
            raise ConfigurationError(msg)

    def _valid_url(self):
        """
        Return True if self.url is valid; false otherwise.
        Should revisit as needed.
        """
        p = urlparse(self.url)
        return p.scheme == 'http' or p.scheme == 'https'

    def loadFromEnvVars(self):
        """Gather configuration from individual environment variables"""
        try:
            self.url = os.environ[RegistryConfiguration.URL_VAR]
        except:
            pass
        try:
            self.username = os.environ[RegistryConfiguration.USERNAME_VAR]
        except:
            pass
        try:
            self.password = os.environ[RegistryConfiguration.PASSWORD_VAR]
        except:
            pass
        try:
            self.def_workspace = os.environ[
                RegistryConfiguration.WORKSPACE_VAR]
        except:
            pass

    def loadFromFile(self):
        """Gather configuration from the configuration file."""
        try:
            with open(self.conf_file) as cf:
                conf = json.load(cf)
        except:
            pass
        try:
            self.url = conf[RegistryConfiguration.URL_FILE]
        except:
            pass
        try:
            self.username = conf[RegistryConfiguration.USERNAME_FILE]
        except:
            pass
        try:
            self.password = conf[RegistryConfiguration.PASSWORD_FILE]
        except:
            pass
        try:
            self.def_workspace = conf[RegistryConfiguration.WORKSPACE_FILE]
        except:
            pass

    def __str__(self):
        return ('url: ' + str(self.url) + '\n' +
                'username: ' + str(self.username) + '\n' +
                'password: ' + str(self.password) + '\n' +
                'workspace: ' + str(self.def_workspace))


class RegistryInterface(object):
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
    URL_FNPARAMS = '/fnparams/'
    URL_LITS = '/literals/'
    URL_CONNS = '/connections/'
    URL_PEIMPLS = '/peimpls/'
    URL_FNIMPLS = '/fnimpls/'

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
    registered_entities = {}  # In-memory cache
    # token = None
    # ###########################
    conf = None

    SSL_CERT_VERIFY = False

    # def __init__(self, conf=None, wspc_id=DEF_WORKSPACE_ID):

    def __init__(self, conf=None):
        """
        Initialise the registry interface module.
        :param conf: a valid RegistryConfiguration object.
        """
        # this imports the requests module before anything else
        # so we don't get a loop when importing
        # requests.get('http://github.com')  # TODO: Why is this needed? Check.

        self.protocol, self.host, self.port = split_url(conf.url)
        self.user = conf.username
        if not conf.def_workspace:
            wspc_name = DEF_WSPC_NAME
        else:
            wspc_name = conf.def_workspace

        self.login(conf.username, conf.password)
        self.conf = conf
        try:
            wspc_id = self._get_workspace_by_name(
                conf.username, wspc_name)['id']
        except:
            wspc_id = self._get_workspace_by_name(
                conf.username, wspc_name)['id']
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
                and (datetime.datetime.now() - self.logged_in_time).seconds <
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
            return RegistryInterface.TYPE_PE
        elif kind == 'functions':
            return RegistryInterface.TYPE_FN
        else:
            return RegistryInterface.TYPE_NOT_RECOGNISED

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
        r = requests.post(url,
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            if r.status_code == requests.codes.forbidden:
                raise NotAuthorisedException()
            else:
                raise
        self.logged_in = r.status_code == requests.codes.ok
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

    def get_workspace_ls(self, workspace_id=None, startswith=''):
        """Gets a listing of the given workspace."""
        workspace_id = workspace_id or self.workspace
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace_id) +
               '/?ls&startswith=' + startswith)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def get_base_url(self):
        """Return the base URL for the registry."""
        return self.protocol + '://' + self.host + (self.port and ':' +
                                                    self.port or '')

    # def set_workspace(self, wspc_id):
    #     self.workspace = wspc_id

    def set_workspace(self, name, owner=None):
        """Set the default workspace to the one identified by the given
        owner and name."""
        if not owner:
            owner = self.conf.username
        wspc = self._get_workspace_by_name(owner, name)
        self.workspace = wspc['id']

    def clone(self, name, orig_workspace_id=None):
        """
        Clone the given workspace into a new one with the name `name`.
        :param name: the name of the new workspace.
        :param orig_workspace_id: the id of the original workspace; defaults
        to the currently active workspace.
        :return the resulting json
        """
        if not self.logged_in:
            raise NotLoggedInError()
            return

        orig_workspace_id = orig_workspace_id or self.workspace

        url = (self.get_base_url() + self.URL_WORKSPACES + '?clone_of=' +
               str(orig_workspace_id))
        r = requests.post(url,
                          data={'name': name},
                          headers=self._get_auth_header(),
                          verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

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

    def get_default_workspace_id(self):
        """
        Return the id of the currently set default workspace.
        """
        return self.workspace

    def get_workspace_info(self, id=None):
        """
        Return the workspace entry id'ed by `id`.
        :param id: the id of the workspace to fetch; defaults to the currently
        selected one.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        id = id or self.workspace

        url = self.get_base_url() + self.URL_WORKSPACES + str(id)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def get_user_info(self, id):
        """
        Retrieve and return user information identified by id
        :param id: the id of the user to fetch information for
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_USERS + str(id)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def _get_by_arbitrary_url(self, url):
        if not self.logged_in:
            raise NotLoggedInError()

        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def search_for_workspaces(self, search_str):
        """
        Search for workspaces satisfying the search_str given.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES +
               '?search=' + search_str)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def search_for_workspace_contents(self, search_str, workspace_id=None):
        """
        Search inside the given workspace for items that satisfy the given
        search_str.
        :param search_str: the string to search for
        :param workspace_id: the id of the workspace to search in, defaults to
        the current default workspace
        """
        if not self.logged_in:
            raise NotLoggedInError()

        workspace_id = workspace_id or self.workspace

        url = (self.get_base_url() + self.URL_WORKSPACES +
               str(workspace_id) + '/?search=' + search_str)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

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
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        if self.PROP_PEIMPLS not in r.json():
            raise NotPEError()

        try:
            impl_url = r.json()[self.PROP_PEIMPLS][impl_index]
        except IndexError:
            raise ImplementationNotFound()
            return

        r = requests.get(impl_url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)
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
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        try:
            return r.json().get('code')
        except:
            raise ImplementationNotFound()

    def _ls(self):
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES +
               str(self.workspace) + '/?ls')
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def ls(self):
        """Return a listing of contents of the currently selected workspace."""

        listing = self._ls()
        print listing

    def _get_workspace_by_name(self, username, wspcname):
        """
        Return the dictionary describing a workspace identified by
        the owner's and the workspace's name.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_WORKSPACES + \
            '?username=' + username + '&name=' + wspcname

        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def get_fn_implementation_code(self, workspace_id, pckg, name,
                                   impl_index=0):
        """
        Return the implementation code of the given function / identified
        by pckg-name.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace_id) +
               '/?fqn=' + pckg + '.' + name)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

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

        r = requests.get(impl_url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)
        return r.json().get('code')

    def register_pe_spec(self, pckg, name, workspace_id=None, descr=''):
        """
        Register a new PE specification or update an existing one.
        :param workspace_id: the id of the workspace; defaults to the default
        workspace
        :param pckg: a string denoting the package of the PE to register.
        :param name: the name of the PE to register.
        :param descr: a textual description of the PE specification.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        workspace_id = workspace_id or self.workspace
        workspace_url = self.get_base_url() + self.URL_WORKSPACES + \
            str(workspace_id) + '/'

        data = {'workspace': workspace_url,
                'pckg': pckg,
                'name': name,
                'description': descr}
        url = self.get_base_url() + self.URL_PES

        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def register_fn_spec(self, pckg, name, return_type,
                         workspace_id=None, descr=''):
        """
        Register a new function specification or update an existing one.
        :param workspace_id: the id of the workspace; defaults to the default
        workspace
        :param pckg: a string denoting the package of the PE to register.
        :param name: the name of the PE to register.
        :param return_type: the type expected to be returned by the function.
        :param descr: a textual description of the PE specification.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        workspace_id = workspace_id or self.workspace
        workspace_url = self.get_base_url() + self.URL_WORKSPACES + \
            str(workspace_id) + '/'
        data = {'workspace': workspace_url,
                'pckg': pckg,
                'name': name,
                'description': descr,
                'return_type': return_type}
        url = self.get_base_url() + self.URL_FNS
        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def add_fn_param(self, name, fnid, ptype=None):
        """
        Attach a function parameter to the function identified by fnid.
        :param name: the name of the parameter
        :param ptype: the type of the parameter
        """
        if not self.logged_in:
            raise NotLoggedInError()

        fn_url = self.get_base_url() + self.URL_FNS + fnid + '/'
        url = self.get_base_url() + self.URL_FNPARAMS

        data = {
            'param_name': name,
            'param_type': ptype,
            'parent_function': fn_url
        }
        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def add_pe_connection(
            self,
            pe_id,
            kind,
            name,
            stype=None,
            dtype=None,
            comment=None,
            is_array=False,
            modifiers=None):
        """
        Add a new connection to an existing PE signature. modifiers should
        be colon-separated string values. pe_id is expected to be of type
        `long` - i.e. just the id, not the URL
        """
        if not self.logged_in:
            raise NotLoggedInError()

        pe_url = self.get_base_url() + self.URL_PES + pe_id + '/'
        url = self.get_base_url() + self.URL_CONNS  # + '/'
        data = {
            'pesig': pe_url,
            'kind': kind,
            'name': name,
            's_type': stype,
            'd_type': dtype,
            'comment': comment,
            'is_array': is_array,
            'modifiers': modifiers}

        # logger.info('New connection data: ' + str(data))
        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def delete_pespec(self, peid):
        """
        Delete the PE specification identified by the given id.
        :param peid: the id of the PE spec to be deleted.
        :return the id of the deleted PE specification.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_PES + peid + '/'
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        for c in r.json()['connections']:
            requests.delete(c,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        for i in r.json()['peimpls']:
            requests.delete(i,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        r = requests.delete(r.json()['url'],
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return peid

    def delete_fnspec(self, fnid):
        """
        Delete the function specification identified by fnid.
        :param fnid: the if of the function to be deleted.
        :return the id of the deleted function specification.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_FNS + fnid + '/'
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        for c in r.json()['parameters']:
            requests.delete(c,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        for i in r.json()['fnimpls']:
            requests.delete(i,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        r = requests.delete(r.json()['url'],
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return fnid

    def delete_peimpl(self, peimpl_id):
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_PEIMPLS + peimpl_id + '/'
        r = requests.delete(url,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return peimpl_id

    # TODO: Test
    def delete_fnimpl(self, fnimpl_id):
        if not self.logged_in:
            raise NotLoggedInError()

        url = self.get_base_url() + self.URL_FNIMPLS + fnimpl_id + '/'
        r = requests.delete(url,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return fnimpl_id

    def delete_pe_connection(self, conn_id):
        """Delete the named connection from the given pe"""
        if not self.logged_in:
            raise NotLoggedInError()

        conn_url = self.get_base_url() + self.URL_CONNS + conn_id + '/'
        r = requests.delete(conn_url,
                            headers=self._get_auth_header(),
                            verify=RegistryInterface.SSL_CERT_VERIFY)

        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return conn_id

    def add_pe_implementation(
            self,
            pe_id,
            code,
            pckg='peimpls',
            name=None,
            description=''):
        """Create a new implementation for the PE identified by `pe_id`."""
        if not self.logged_in:
            raise NotLoggedInError()

        # Retrieve the corresponding PE
        pe_url = self.get_base_url() + self.URL_PES + pe_id + '/'
        pereq = requests.get(pe_url,
                             headers=self._get_auth_header(),
                             verify=RegistryInterface.SSL_CERT_VERIFY)
        if pereq.status_code != requests.codes.ok:
            pereq.raise_for_status()

        if name is None:
            name = pereq.json()['name'] + '_IMPL_' + str(datetime.date.today())

        workspace = pereq.json()['workspace']
        url = self.get_base_url() + self.URL_PEIMPLS
        data = {'description': description,
                'code': code,
                'parent_sig': pe_url,
                'pckg': pckg,
                'name': name,
                'workspace': workspace}
        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def add_fn_implementation(
            self,
            fn_id,
            code,
            pckg='fnimpls',
            name=None,
            description=''):
        """
        Create a new implementation for the function identified by
        `fn_id`.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        # Retrieve the corresponding function
        fn_url = self.get_base_url() + self.URL_FNS + fn_id + '/'
        fnreq = requests.get(fn_url,
                             headers=self._get_auth_header(),
                             verify=RegistryInterface.SSL_CERT_VERIFY)
        if fnreq.status_code != requests.codes.ok:
            fnreq.raise_for_status()

        if not name:
            name = fnreq.json()['name'] + '_IMPL_' + str(datetime.date.today())

        workspace = fnreq.json()['workspace']
        print 'workspace:', str(workspace)
        url = self.get_base_url() + self.URL_FNIMPLS
        print 'url:', url
        data = {'description': description,
                'code': code,
                'parent_sig': fn_url,
                'pckg': pckg,
                'name': name,
                'workspace': workspace}
        print '\n\nDATA:', str(data)
        r = requests.post(url,
                          headers=self._get_auth_header(),
                          data=data,
                          verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        return r.json()

    def get_by_name(self, fqn, workspace=None, kind=None):
        """
        Get an object by fqn.
        :param fqn: the fqn of the object to fetch, which is pckg.name
        :param workspace: the workspace in which to operate; defaults to the
        interface's default workspace.
        :param kind: filter by the kind of the object to be fetched;
        takes values RegistryInterface.[TYPE_PE|TYPE_FN]. By default it does
        not filter.
        """
        if not self.logged_in:
            raise NotLoggedInError()

        workspace = workspace or self.workspace
        url = (self.get_base_url() + self.URL_WORKSPACES + str(workspace) +
               '/?fqn=' + fqn)
        r = requests.get(url,
                         headers=self._get_auth_header(),
                         verify=RegistryInterface.SSL_CERT_VERIFY)
        if r.status_code != requests.codes.ok:
            r.raise_for_status()

        j = r.json()
        if not kind:
            return j

        jtype = self._extract_kind_from_json_object(j)
        if kind == jtype:
            return j

        return None


##############################################################################
# Utility and static methods:
##############################################################################


def remove_registry_from_meta_path():
    mylist = [i for i in sys.meta_path if type(i) != RegistryInterface]
    sys.meta_path = mylist


def currentRegistry():
    """
    Returns the currently used registry instance.
    """
    for i in sys.meta_path:
        if isinstance(i, RegistryInterface):
            return i

DEF_WORKSPACE_ID = 1
DEF_WSPC_NAME = 'ROOT'


def initRegistry(username=None, password=None, url=None,
                 workspace=DEF_WORKSPACE_ID, token=None):
    """
    Initialises the registry. This method must be called before any 'import'
    statements.
    """
    remove_registry_from_meta_path()
    reg = RegistryInterface()
    reg.workspace = workspace
    if url:
        reg.protocol, reg.host, reg.port = split_url(url)
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


def initRegistryFromConf(conf):
    """
    Initialise and return a Registry instance given the configuration object.
    If the configuration does not specify a default workspace, default to the
    ROOT workspace.
    :param conf: A valid RegistryConfiguration instance
    """
    remove_registry_from_meta_path()
    reg = RegistryInterface()
    reg.protocol, reg.host, reg.port = split_url(conf.url)
    reg.user = conf.username
    if not conf.def_workspace:
        wspc_name = DEF_WSPC_NAME
    else:
        wspc_name = conf.def_workspace

    reg.login(conf.username, conf.password)
    wspc_id = reg._get_workspace_by_name(conf.username, wspc_name)['id']
    reg.workspace = wspc_id

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


class ImplementationNotFound(VerceRegClientLibError):
    pass


class NotPEError(VerceRegClientLibError):
    pass


class NotFunctionError(VerceRegClientLibError):
    pass


class NotAuthorisedException(VerceRegClientLibError):
    pass


class UnknownPackageException(VerceRegClientLibError):
    pass


class RegistrationFailed(VerceRegClientLibError):
    pass


class ConfigurationError(VerceRegClientLibError):
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
    rconf = RegistryConfiguration()

    reg = initRegistryFromConf(rconf)
    print reg.get_workspace_ls()

    # print 'Main for testing starting'
#     reg = initRegistry(username='iraklis', password='iraklis')
#     print reg.find_module(fullname='pes.RandomWordProducer')

if __name__ == '__main__':
    main()
