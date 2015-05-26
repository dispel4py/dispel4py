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

import core
from core import RegistryInterface
from core import RegistryConfiguration
from registry_importer import RegistryImporter

import importlib
import inspect
import sys
import requests  # for exceptions and errors

from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import TerminalFormatter

try:
    regconf = RegistryConfiguration()
    regint = RegistryInterface(regconf)
    regimporter = RegistryImporter(regint)
except:
    print 'Error when instantiating the registry interface.'
    print 'Please check your configuration and credentials are correct and try'
    print 'again.'
    sys.exit(1)


class bcolors:
    """
    Support for coloured output on the terminal.
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


general_help = """
\033[1mCommands:\033[0m
    \033[4mhelp([command])\033[0m\
: Outputs help for the dispel4py registry's interactive interface.
    \033[4minfo()\033[0m: Outputs information about the current session.
    \033[4mset_workspace(name [, owner])\033[0m\
: Change the active workspace to the one
        identified by name and optionally owner.
    \033[4mset_workspace_by_id(wspcid)\033[0m\
: Change the active workspace to the one
        indicated by wspcid.
    \033[4mmk_workspace(name[, description])\033[0m\
: Create a new, empty workspace.
        Optionally, provide a description.
    \033[4mclone(name[, description, append])\033[0m\
: Clone the currently active workspace
        into a new one with the given name. Optionally provide a description
        as well as choose whether the description to replace the existing one,
        or to be appended to it.
    \033[4mcopy(item_name, to_workspace_name[, username, target_name])\033[0m\
: Copy the
        given item into another workspace. It works for PEs, functions and
        literals.
    \033[4mwls([name, owner, startswith])\033[0m\
: List the contents of a workspace; defaults
        to the currently active one.
    \033[4mfind_my_workspaces\033[0m\
: Find the workspaces owned by the logged in user.
    \033[4mfind_workspaces(str)\033[0m\
: Searches for workspaces that satisfy the given text
        query.
    \033[4mfind_in_workspace(str)\033[0m\
: Searches inside the currently active workspace for
        entities that satisfy the given text query.
    \033[4mregister_fn(name, impl_subpackage)\033[0m\
: Register a function. The name should be
        the full python name and it should be importable by Python.
    \033[4mregister_pe(name, impl_subpackage)\033[0m\
: Register a processing element. The name
        should be the full python name and it should be importable by Python.
    \033[4mregister_literal(pckg, name, value[, description])\033[0m\
: register a new literal
        under pckg.name. This is equivalent to registering a module pckg.name
        containing `name` = "`value`".
    \033[4mrm(name)\033[0m\
: Delete the given named item, as well as its associated items, in
        the currently active workspace.
    \033[4mview(name)\033[0m\
: View details about the given named item.
"""


def help(command=None):
    """
    Usage: help(['command'])
    Print help text.
    """
    # Voodooesque but it works for now.
    import dispel4py
    import dispel4py.registry
    import dispel4py.registry.registry
    if not command:
        print general_help
    else:
        try:
            print getattr(dispel4py.registry.registry, command).__doc__.strip()
        except:
            print 'Unknown command: ' + command


def set_workspace(name, owner=None):
    """
    Set the current workspace.
    :param name: the name of the workspace to change to
    :param owner: the owner of the workspace to change to. This defaults
    to the current user.
    """
    owner = owner or regconf.username
    try:
        regint.set_workspace(name, owner)
        print 'Default workspace set to: ' + name + ' (' + owner + ') ' +\
              '[' + str(regint.workspace) + ']'
    except:
        print 'Workspace ' + name + ' (' + owner + ') not found.'


def set_workspace_by_id(wspcid):
    """
    Set the current workspace by id.
    :param wspcid: the id of the workspace to switch to.
    """
    try:
        regint.workspace = long(wspcid)
    except:
        print 'Invalid id ' + str(wspcid)


def _prntbln(name):
    if name == 'pes':
        return 'PEs'
    if name == 'functions':
        return 'Functions'
    if name == 'literals':
        return 'Literals'
    if name == 'peimpls':
        return 'PE Implementations'
    if name == 'fnimpls':
        return 'Function Implementations'
    if name == 'packages':
        return 'Packages'


def wls(name=None, owner=None, startswith=''):
    """
    Return a string listing the contents of the workspace given as a
    parameter. It default to the current default workspace.
    :param name: the human-readable name of the workspace
    :param owner: the username of the owner of the workspace
    :param startswith: the prefix of the entities to be listed
    """
    wid = None
    if not name:
        if owner:
            # cannot resolve workspace
            print 'Cannot resolve workspace for specific user without' +\
                  ' a workspace name'
            return
        else:
            wid = regint.get_default_workspace_id()
    else:  # a name is provided
        if not owner:
            owner = regconf.username
        try:
            wid = regint._get_workspace_by_name(owner, name)['id']
        except:
            print 'Cannot resolve workspace with name "' +\
                  name + '" owned by "' + owner + '"'
            return

    try:
        listing = regint.get_workspace_ls(wid, startswith)
    except:
        print 'Unable to get the workspace listing'
        return

    for t in ['pes', 'peimpls', 'functions',
              'fnimpls', 'literals', 'packages']:
        if len(listing[t]) > 0:
            print __header('[' + _prntbln(t) + ']')
            for i in listing[t]:
                if t == 'packages':
                    print i
                else:
                    for j in i:
                        print '(' + j + ')', i[j]


def __header(s):
    return bcolors.BOLD + s + bcolors.ENDC


def __ul(s):
    return bcolors.UNDERLINE + s + bcolors.ENDC


def info():
    """
    Print information regarding the current session.
    """
    try:
        info = regint.get_workspace_info()
    except:
        print 'Could not retrieve information for active workspace (' +\
              'id=' + str(regint.workspace) + ').'
        return
    try:
        userinfo = regint._get_by_arbitrary_url(info['owner'])
    except:
        userinfo = None
    wurl = info['url']
    wname = info['name']
    if userinfo:
        wowner = userinfo['username']
    else:
        wowner = info['owner']
    try:
        durl = regint._get_workspace_by_name(
            regconf.username, regconf.def_workspace)['url']
        dname = regconf.def_workspace
        downer = regconf.username
    except:
        durl = 'N/A'
        dname = 'N/A'
        downer = 'N/A'
    dname = regconf.def_workspace
    downer = regconf.username
    # wdescription = info['description']
    print __header('[Current workspace]')
    print '(' + wurl + ') ' + wname + ' - ' + wowner
    # print '[Workspace description]'
    # print _short_descr(wdescription, 75)
    print __header('[Default workspace]')
    print '(' + durl + ') ' + dname + ' - ' + downer
    print __header('[Registry endpoint]')
    print regconf.url
    print __header('[Registry user]')
    print regconf.username


def get_logged_in_username():
    print regconf.username


def _reload_module(modulestr):
    """
    Iteratively reload all dot-separated modules in the given string.
    :param modulestr: a dot-separated string denoting a module
    :return the longest module imported
    """
    pos = -1
    mods = []
    for i in range(0, modulestr.count('.')):
        pos = modulestr.find('.', pos + 1)
        mods.append(modulestr[:pos])
    mods.append(modulestr)

    for mstr in mods:
        m = importlib.import_module(mstr)
        reload(m)
    return m


def _reload():
    _reload_module('dispel4py.registry')
    _reload_module('dispel4py.registry.core')
    _reload_module('dispel4py.registry.registry_importer')
    _reload_module('dispel4py.registry.registry')


def _clean_gen_docstr(s):
    """
    Clean up the general documentation of a docstring.
    :param s the string to be stripped of extra invisible characters.
    """
    s = s.strip()
    if s == '':
        return ''
    ret = ''
    for t in s.split('\n'):
        ret += t.lstrip() + '\n'
    return ret


def _extract_meta_by_docstr(s):
    """
    Extract relevant metadata from the python docstring given.
    :param s: the docstring to be parsed
    :return a dictionary
    """
    toret = {'description': None, 'type': {'name': None,
                                           'description': None},
             'inputs': [], 'outputs': [],
             'params': [], 'return': {}}
    fcpos = s.find(':')
    gendoc = _clean_gen_docstr(s[:fcpos])
    for l in s[fcpos:].split('\n'):  # Assume one doc item by line
        l = l.strip()
        # print l
        if l.startswith(':name'):
            toret['type'].update(_extract_docstr_name_line(l))
        elif l.startswith(':input'):
            toret['inputs'].append(_extract_docstr_inout_line(l))
        elif l.startswith(':output'):
            toret['outputs'].append(_extract_docstr_inout_line(l))
        elif l.startswith(':return'):
            toret['return'].update(_extract_docstr_return_line(l))
        elif l.startswith(':param'):
            toret['params'].append(_extract_docstr_param_line(l))
        else:
            pass  # Ignore other lines
    toret['description'] = gendoc
    return toret


def _extract_docstr_param_line(s):
    """
    Extract relevant information from a param comment line.
    :return a dictionary
    """
    name = None
    t = None
    descr = None
    s = s[len(':param'):].lstrip()

    # Extract the name:
    cpos = s.find(':')
    name = s[:cpos]
    s = s[cpos+1:].lstrip()
    descr = s

    # Extract the type if present
    if s.startswith('<'):
        closepos = s.find('>')
        t = s[1:closepos]
        descr = s[closepos+1:].strip()

    return {'name': name, 'type': t, 'description': descr}


def _extract_docstr_return_line(s):
    """
    Extract relevant information from a return comment line
    :return a dictionary
    """
    s = s[len(':return'):].lstrip()
    t = None
    d = s
    if s.startswith('<'):
        closepos = s.find('>')
        t = s[1:closepos]
        d = s[closepos+1:].strip()
    return {'type': t, 'description': d}


def _extract_docstr_name_line(s):
    """
    Extract relevant information from a name line in a docstring.
    :return a dictionary
    """
    spos = s.find(':name') + 5  # len(':name')
    srest = s[spos:].strip()
    fspos = srest.find(' ')
    fullname = (srest + ' ')[:fspos]
    pckg, name = core.split_name(fullname)
    if fspos > -1:
        descr = srest[fspos + 1:]
    else:
        descr = None
    return {'name': {'pckg': pckg or None, 'name': name or None},
            'description': descr}


# TODO Rewrite using REs?
def _extract_docstr_inout_line(s):
    """
    Extract relevant information from a parameter line in a docstring.
    :return a dictionary
    """
    s = s.strip()
    offset = 0
    kind = None
    if s.startswith(':input'):
        offset = 6
        kind = 'IN'
    elif s.startswith(':output'):
        offset = 7
        kind = 'OUT'
    else:  # should never reach here
        raise Exception('Unknown keyword starting line "' + s + '"')

    srest = s[offset:].strip()

    # Get the connection name:
    offset = srest.find(':')
    if offset < 1:
        raise Exception('Connection does not have a name')
    name = srest[:srest.find(':')]
    srest = srest[offset+1:].strip()

    schar = None
    fchar = None
    isArray = False
    stype = None
    dtype = None
    descr = None

    # Get the stype, if specified:
    if srest.startswith('<'):
        schar = '<'
        fchar = '>'
    elif srest.startswith('['):
        schar = '['
        fchar = ']'
        isArray = True
    if schar:
        closepos = srest.find(fchar)
        stype = srest[1:closepos]
        srest = srest[closepos+1:].strip()

    # Get the dtype, if specified:
    if srest.startswith('<'):
        closepos = srest.find('>')
        dtype = srest[1:closepos]
        descr = srest[closepos+1:].strip()
    else:
        descr = srest.strip()

    ret = {'kind': kind, 'name': name, 'isArray': isArray, 'stype': stype,
           'dtype': dtype, 'description': descr}
    return ret


def _find_earliest_doc_kw(s, beg=0):
    """
    Find the earliest instance of a keyword in the given string and
    return its position. This uses the `find` method of str.
    :param s: the string to be scanned.
    :return the earliest position of a match, -1 otherwise.
    """
    kws = [':name', ':input', ':output']
    pos = [s.find(k) for k in kws]
    return min(pos)


def find_workspaces(search_str=''):
    """
    Find workspaces that match the given search string.
    :param search_str: the search string to search for.
    """
    search_str = str(search_str)
    res = regint.search_for_workspaces(search_str)
    for i in res:
        print '(' + i['url'] + ') ' + i['name'] + ': ' +\
              _short_descr(i['description'])
    print 'Total:', str(len(res))


def find_my_workspaces():
    """
    Find the workspaces owned by the logged in user.
    """
    res = regint.get_workspaces_by_user_and_name(regconf.username)
    count = 0
    for i in res:
        print '(' + i['url'] + ') ' + i['name'] + ': ' +\
              _short_descr(i['description'])
        count += 1
    print 'Total: ' + str(count)


def find_in_workspace(search_str=''):
    """
    Search for workspace contents in the given workspace. If a workspace
    if is not given, search in the currently selected workspace.
    :param search_str: the string to search for in the workspace items.
    """
    search_str = str(search_str)
    if search_str.strip() == '':
        print 'Empty search string provided. ' + \
              'Please provide text to search for.'
        return
    res = regint.search_for_workspace_contents(search_str)

    for i in res:
        print '(' + str(i['url']) + ')', i['pckg'] + '.' +\
              i['name'] + ':', _short_descr(i['description'])
    print 'Total:', str(len(res))


# TODO: Add registration policy in case of name clash (replace, etc.)
def register_fn(name, impl_subpackage='_impls'):
    """
    Register the contents of the file named `filename` as a function
    signature and associated implementation in the registry.
    This makes use of Python documentation to extract the required
    metadata. Please consult the documentation for more information.
    :param name: the full name of the function to be registered. This
    assumes that there is at least one '.' separating a module name to
    the class name (the right-most part being the function name).
    :param impl_subpackage: the package suffix to be used for the
    implementation registration.
    """
    dotpos = name.rfind('.')
    if dotpos < 1:
        print name + ' does not appear to be a valid name'
        return

    module = name[:dotpos]
    fnname = name[dotpos+1:]
    try:
        m = _reload_module(module)
        code = inspect.getsource(m)
        fn = getattr(m, fnname)
    except ImportError:
        print 'Cannot find module ' + module
        return
    except AttributeError:
        print 'Cannot find ' + fnname + ' in ' + module
        return

    doc = fn.__doc__.strip()

    meta = _extract_meta_by_docstr(doc)
    already_present = False
    try:
        if not meta['return']['type']:
            raise KeyError()  # to signify a documentation error
        fns = regint.register_fn_spec(meta['type']['name']['pckg'],
                                      meta['type']['name']['name'],
                                      meta['return']['type'],
                                      descr=meta['description'])

        # The parameters:

        fnid = fns['id']
        for p in meta['params']:
            regint.add_fn_param(p['name'], str(fnid), p['type'])
    except KeyError:
        print 'Incomplete function documentation.'
        return
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            print 'Insufficient permissions'
            return  # Abort the registration completely
        # elif e.response.status_code == 400:
        #     print 'Failure possibly due to incomplete documentation.'
        #     return

        # The function could already be registered, so retrieve it
        fqn = (meta['type']['name']['pckg'] + '.' +
               meta['type']['name']['name'])
        # try:
        fns = regint.get_by_name(fqn, kind=RegistryInterface.TYPE_FN)
        fnid = fns['id']
        already_present = True
        # except:
        #     print 'Registration failed.'
        #     return

    # Register the implementation and associate it to the spec above
    try:
        regint.add_fn_implementation(
            str(fnid),
            code,
            pckg=meta['type']['name']['pckg'] + '.' + impl_subpackage,
            name=meta['type']['name']['name'],
            description=meta['description'])
    except:
        pass

    if already_present:
        print 'Function already registered: ' + fns['url']
    else:
        print 'Registered function: ' + fns['url']


# TODO: Add registration policy in case of name clash
def register_pe(name, impl_subpackage='_impls'):
    """
    Register the contents of the file named `filename` as a PE signature
    in the registry. This makes use of Python documentation to extract
    the required metadata. Please consult the documentation for more
    information.
    :param name: the full name of the PE class to be registered. This
    assumes that there is at least one '.' separating a module name to
    the class name (the right-most part being the class name).
    :param impl_subpackage: the package suffix to be used for the
    implementation registration.
    """
    dotpos = name.rfind('.')
    if dotpos < 1:
        print name + ' does not appear to be a valid name'
        return

    module = name[:dotpos]
    pename = name[dotpos+1:]
    try:
        m = _reload_module(module)
        pe = getattr(m, pename)
    except ImportError:
        print 'Cannot find module ' + module
        return
    except AttributeError:
        print 'Cannot find ' + pename + ' in ' + module
        return

    doc = pe.__doc__.strip()

    meta = _extract_meta_by_docstr(doc)
    already_present = False

    # Actual registrations via the core module here:
    try:
        # The spec:
        pes = regint.register_pe_spec(meta['type']['name']['pckg'],
                                      meta['type']['name']['name'],
                                      descr=meta['description'])
        # The connections:
        peid = pes['id']
        for c in meta['inputs'] + meta['outputs']:
            regint.add_pe_connection(
                str(peid),
                kind=c['kind'],
                name=c['name'],
                stype=c['stype'],
                dtype=c['dtype'],
                comment=c['description'],
                is_array=c['isArray'],
                modifiers=None)
    except KeyError:
        print 'Incomplete function documentation.'
        return
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            print 'Insufficient permissions'
            return  # Abort the registration completely

        #  The PE could already be registered; attempt to fetch it
        fqn = (meta['type']['name']['pckg'] + '.' +
               meta['type']['name']['name'])
        try:
            pes = regint.get_by_name(fqn, kind=RegistryInterface.TYPE_PE)
            peid = pes['id']
            already_present = True
        except:
            print 'Registration failed.'
            return
    except:
        print 'An unknown error has occurred.'
        return

    # The implementation:
    code = inspect.getsource(m)
    try:
        regint.add_pe_implementation(
            str(peid),
            code=code,
            pckg=meta['type']['name']['pckg'] + '.' + impl_subpackage,
            name=meta['type']['name']['name'],
            description=meta['description'])
    except:
        # Do not do anything if a PEImpl of same fqn is already present
        pass

    if already_present:
        print 'PE already registered: ' + pes['url']
    else:
        print 'Registered PE: ' + pes['url']


def register_literal(pckg, name, value, description=''):
    """
    Register a new literal.
    :param pckg: the package
    :param name: the name of the literal
    :param value: the value of the literal - this will be turned into a str
    :param description: a description for the literal
    """
    pckg = pckg.strip()
    name = name.strip()
    description = description.strip()
    value = str(value)

    ret = None
    try:
        ret = regint.register_literal(pckg, name, value, None, description)
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            print 'Insufficient permissions'
        else:
            try:
                # Check if it's already present
                lit = regint.get_by_name(pckg + '.' + name,
                                         kind=RegistryInterface.TYPE_LIT)
                lit['id']
                print 'Literal already registered.'
            except:
                print 'An unknown error has occurred (' +\
                      str(e.response.status_code) + ')'
        return
    except:
        print 'An unknown error has occurred'
        return
    print 'Registered literal: ' + ret['url']


def rm(name):
    """
    Delete the given named item, as well as its associated items, in the
    currently active workspace.
    :param name: the pckg.name name of the item to delete.
    """
    name = str(name).strip()
    if name == '':
        print 'Item name not provided.'
        return
    j = None
    try:
        j = regint.get_by_name(name)
    except:
        print 'Item ' + name + \
              ' could not be found in the active workspace.'
        return

    type = regint._extract_kind_from_json_object(j)
    if type != RegistryInterface.TYPE_NOT_RECOGNISED:
        try:
            regint.delete_by_url(j['url'])
            print 'Deleted ' + name + ' (' + j['url'] + ')'
        except requests.HTTPError as e:
            if e.response.status_code == 403:
                print 'Insufficient permissions'
            else:
                print 'An unknown error has occurred (' +\
                      str(e.response.status_code) + ')'
            return
        except:
            print 'An unknown error has occurred'
            return
    else:
        print 'Could not recognise the type of ' +\
              name + ' (' + j['url'] + ')'


def mk_workspace(name, description=None):
    """
    Create a new, empty workspace.
    :param name: The name of the workspace
    :param description: The description of the workspace
    """
    try:
        r = regint.mk_workspace(name, description)
        print 'New workspace created: ' + str(r['url'])
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            print 'Insufficient permissions'
            return
        else:
            try:
                w = regint._get_workspace_by_name(regconf.username, name)
                if 'id' in w:
                    print 'Workspace ' + name + ' (' + regconf.username + ')' +\
                        ' already exists.'
                else:
                    print 'Could not create workspace (' +\
                          str(e.response.status_code) + ')'
            except:
                print 'Could not create workspace (' +\
                      str(e.response.status_code) + ')'
    except:
        print 'Could not create workspace.'


def clone(name, description=None, append=False):
    """
    Clone the currently active workspace into a new one named `name`.
    :param name: the name of the new workspace to clone the currently active
    one into.
    :param description: optionally, of the description the new workspace.
    :param append: whether to append to the existing documentation of replace
    it completely.
    """
    try:
        r = regint.clone(name)
        if description:  # Update/replace the description accordingly
            if not append:
                r['description'] = description
            else:
                r['description'] += '\n\n' + description
            r = regint.put_item(r['url'], r)
        print 'New workspace created: ' + str(r['url'])
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            print 'Insufficient permissions'
            return
        else:
            print 'Workspace cloning failed (' +\
                  str(e.response.status_code) + ')'
            return
    except:
        # See if there is already a workspace with the same name under the
        # current user and notify accordingly
        try:
            w = regint._get_workspace_by_name(regconf.username, name)
            if 'id' in w:
                print 'Workspace ' + name + ' (' + regconf.username + ')' +\
                    ' already exists.'
            else:
                print 'Workspace cloning failed.'
        except:
            print 'Workspace cloning failed.'


def display_pe(j):
    """
    Display the PE information contained in the json object j.
    :param j: the PE to display in json format.
    """
    print __header('Name: ') + j['pckg'] + '.' + j['name']
    print __header('URL: ') + j['url']
    if j['description'].strip() != '':
        print __header('Description: ') + _short_descr(j['description'])
    if j['clone_of'] and j['clone_of'].strip() != '':
        print __header('Origin: ') + j['clone_of']
    print __header('Implementations:')
    for i in j['peimpls']:
        try:
            peimpl = regint._get_by_arbitrary_url(i)
            print '(' + peimpl['url'] + ') ' + \
                  peimpl['pckg'] + '.' + peimpl['name']
        except:
            pass


def display_fn(j):
    """
    Display the PE information contained in the json object j.
    :param j: the PE to display in json format.
    """
    print __header('Name: ') + j['pckg'] + '.' + j['name']
    print __header('URL: ') + j['url']
    if j['description'].strip() != '':
        print __header('Description: ') + _short_descr(j['description'])
    if j['clone_of'] and j['clone_of'].strip() != '':
        print __header('Origin: ') + j['clone_of']
    print __header('Implementations:')
    for i in j['fnimpls']:
        try:
            fnimpl = regint._get_by_arbitrary_url(i)
            print '(' + fnimpl['url'] + ') ' + \
                  fnimpl['pckg'] + '.' + fnimpl['name']
        except:
            pass


def display_lit(j):
    """
    Display the PE information contained in the json object j.
    :param j: the PE to display in json format.
    """
    print __header('Name: ') + j['pckg'] + '.' + j['name']
    print __header('URL: ') + j['url']
    if j['description'].strip() != '':
        print __header('Description: ') + _short_descr(j['description'])
    if j['clone_of'] and j['clone_of'].strip() != '':
        print __header('Origin: ') + j['clone_of']
    print __header('Value: ') + j['value']


def display_peimpl(j):
    """
    Display the PE information contained in the json object j.
    :param j: the PE to display in json format.
    """
    # Get the parent sig
    parent_name = 'Unknown!'
    try:
        p = regint._get_by_arbitrary_url(j['parent_sig'])
        parent_name = p['pckg'] + '.' + p['name']
    except:
        pass
    print __header('Name: ') + j['pckg'] + '.' + j['name']
    print __header('URL: ') + j['url']
    if j['description'].strip() != '':
        print __header('Description: ') + _short_descr(j['description'])
    if j['clone_of'] and j['clone_of'].strip() != '':
        print __header('Origin: ') + j['clone_of']
    print __header('Implements PE: ') + parent_name
    print __header('Code:')
    print _pretty_code(j['code'])


def display_fnimpl(j):
    """
    Display the PE information contained in the json object j.
    :param j: the PE to display in json format.
    """

    # Get the parent sig
    parent_name = 'Unknown!'
    try:
        p = regint._get_by_arbitrary_url(j['parent_sig'])
        parent_name = p['pckg'] + '.' + p['name']
    except:
        pass
    print __header('Name: ') + j['pckg'] + '.' + j['name']
    print __header('URL: ') + j['url']
    if j['description'].strip() != '':
        print __header('Description: ') + _short_descr(j['description'])
    if j['clone_of'] and j['clone_of'].strip() != '':
        print __header('Origin: ') + j['clone_of']
    print __header('Implements function: ') + parent_name
    print __header('Code:')
    print _pretty_code(j['code'])


def _pretty_code(c):
    """
    Pretty-print python code (or turn off pp for different applications).
    """
    return highlight(c, PythonLexer(), TerminalFormatter())


def view(name):
    """
    Print details of the given component identified by `name` within the
    currently active workspace.
    :param name: the pckg.name of the component to view.
    """
    name = name.strip()

    j = None
    try:
        j = regint.get_by_name(name)
    except:
        print 'Item ' + name + \
              ' could not be retrieved from the active workspace'
        return

    type = regint._extract_kind_from_json_object(j)
    type == RegistryInterface.TYPE_PE and display_pe(j)
    type == RegistryInterface.TYPE_FN and display_fn(j)
    type == RegistryInterface.TYPE_LIT and display_lit(j)
    type == RegistryInterface.TYPE_PEIMPL and display_peimpl(j)
    type == RegistryInterface.TYPE_FNIMPL and display_fnimpl(j)


def _short_descr(s, length=30):
    ret = str(s).strip()
    ret = ret.replace('\n', ' ').replace('\r', '')
    if len(ret) > length:
        ret = ret[:length].strip() + ' [...]'
    return ret


def copy(item_name, to_wspc, wspc_owner=None, target_name=None):
    """
    Copy an item - a Literal, a PE or a function - as well as its associated
    objects (PE and function implementations) to another workspace.
    :param item_name: The pckg.name of the item to copy
    :param to_wspc: The name of the target workspace.
    :param wspc_owner: The username of the target workspace owner; defaults
    to the current user.
    :param target_name: Optionally, an alternative pckg.name of the item to
    copy. In the current registry implementation, in the case of functions and
    PEs, their respective implementations will be stored under the package
    pckg.implementations in order to avoid name clashes.
    """
    wspc_owner = wspc_owner or regconf.username

    target_wspc_id = None
    try:
        target_wspc = regint._get_workspace_by_name(wspc_owner, to_wspc)
        target_wspc_id = target_wspc['id']
    except:
        print 'Invalid or inaccessible target workspace'
        return

    src = None
    src_url = None
    try:
        src = regint.get_by_name(item_name)
        src_url = src['url']
    except:
        print 'Could not find item ' + item_name + ' in the active workspace'
        return
    src_type = regint._extract_kind_from_json_object(src)
    if (src_type != RegistryInterface.TYPE_PE and
            src_type != RegistryInterface.TYPE_FN and
            src_type != RegistryInterface.TYPE_LIT):
        print 'Item ' + item_name + ' cannot be copied.'
        print 'It needs to be a PE or a function or a literal.'
        return

    copy_url = src_url + '?copy_to=' + str(target_wspc_id)
    if target_name:
        copy_url += '&target_name=' + target_name

    clone = None
    try:
        clone = regint._get_by_arbitrary_url(copy_url)
        print 'Created ' + clone['pckg'] + '.' + clone['name'] + \
              ' in workspace ' + to_wspc + ' (' + wspc_owner + ')'
    except requests.HTTPError as e:
        if e.response.status_code == 500:
            print 'Copying of ' + item_name + \
                  ' failed due to an integrity error.'
            print 'Please check for naming clashes in the target workspace.'
        else:
            print 'Copying of ' + item_name + ' failed.'


# main() is only for quick tests
def main():
    register_pe("tests.rand_word.RandomWordProducer")
    wls()
    find_in_workspace('desc')

    find_workspaces('n')
    print '---'

    find_workspaces('new')

    print '---'
    find_workspaces('root')

    set_workspace('NEW_TEST')
    find_in_workspace('desc')

    set_workspace('NEW_TEST2')
    find_in_workspace('desc')

    # print 'WORKSPACE 3:'
    # set_workspace_by_id(3)
    # wls()
    #
    # print 'WORKSPACE 1:'
    # set_workspace_by_id(1)
    # wls()
    #
    # info()
    # set_workspace('NEW_TEST')
    # info()

    set_workspace('ROOT')
    clone('root_clone3')

    find_workspaces()

if __name__ == '__main__':
    main()
