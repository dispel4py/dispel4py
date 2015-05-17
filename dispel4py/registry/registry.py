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


regconf = RegistryConfiguration()
regint = RegistryInterface(regconf)
regimporter = RegistryImporter(regint)


general_help = """
Commands:
    help([command]): outputs help for the dispel4py registry's interactive
                     interface.
    set_workspace(name [, owner]): change the active workspace to the one
                                   identified by name and optionally owner.
    set_workspace_by_id(wspcid): change the active workspace to the one
                                 indicated by wspcid.
    clone(name): clone the currently active workspace into a new one with the
                 given name.
    wls([name, owner, startswith]): list the contents of a workspace; defaults
                                    to the currently active one.
    info(): outputs information about the current session.
    find_workspaces(str): searches for workspaces that satisfy the given text
                          query.
    find_in_workspace(str): searches inside the currently active workspace for
                            entities that satisfy the given text query.
    register_fn(name, impl_subpackage): register a function. The name should be
                                        the full python name and it should be
                                        importable by Python.
    register_pe(name, impl_subpackage): register a processing element. The name
                                        should be the full python name and it
                                        should be importable by Python.
"""


def help(command=None):
    """
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
        print 'Default workspaces set to: ' + name + ' (' + owner + ') ' +\
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

    listing = regint.get_workspace_ls(wid, startswith)

    for t in ['pes', 'peimpls', 'functions',
              'fnimpls', 'literals', 'packages']:
        if len(listing[t]) > 0:
            print '[' + _prntbln(t) + ']'
            for i in listing[t]:
                if t == 'packages':
                    print i
                else:
                    for j in i:
                        print '(' + j + ')', i[j]


def info():
    """
    Print information regarding the current session.
    """
    info = regint.get_workspace_info()
    userinfo = regint._get_by_arbitrary_url(info['owner'])
    wurl = info['url']
    wname = info['name']
    wowner = userinfo['username']
    durl = regint._get_workspace_by_name(
        regconf.username, regconf.def_workspace)['url']
    dname = regconf.def_workspace
    downer = regconf.username
    # wdescription = info['description']
    print '[Current workspace]'
    print '(' + wurl + ') ' + wname + ' - ' + wowner
    # print '[Workspace description]'
    # print _short_descr(wdescription, 75)
    print '[Default workspace]'
    print '(' + durl + ') ' + dname + ' - ' + downer
    print '[Registry endpoint]'
    print regconf.url
    print '[Registry user]'
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
        # print 'Importing and reloading "' + mstr + '"'
        m = importlib.import_module(mstr)
        reload(m)
    return m


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
    res = regint.search_for_workspaces(search_str)
    for i in res:
        # print i['url']
        print '(' + i['url'] + ') ' + i['name'] + ': ' +\
              _short_descr(i['description'])
    print 'Total:', str(len(res))


def find_in_workspace(search_str):
    """
    Search for workspace contents in the given workspace. If a workspace
    if is not given, search in the currently selected workspace.
    :param search_str: the string to search for in the workspace items.
    """
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
    m = _reload_module(module)
    code = inspect.getsource(m)
    fn = getattr(m, fnname)
    doc = fn.__doc__.strip()

    meta = _extract_meta_by_docstr(doc)
    already_present = False
    try:
        fns = regint.register_fn_spec(meta['type']['name']['pckg'],
                                      meta['type']['name']['name'],
                                      meta['return']['type'],
                                      descr=meta['description'])

        # The parameters:
        fnid = fns['id']
        for p in meta['params']:
            regint.add_fn_param(p['name'], str(fnid), p['type'])
    except:
        # The function could already be registered, so retrieve it
        fqn = (meta['type']['name']['pckg'] + '.' +
               meta['type']['name']['name'])
        fns = regint.get_by_name(fqn, kind=RegistryInterface.TYPE_FN)
        fnid = fns['id']
        already_present = True

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
    m = _reload_module(module)
    pe = getattr(m, pename)
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
    except:
        #  The PE could already be registered; attempt to fetch it
        fqn = (meta['type']['name']['pckg'] + '.' +
               meta['type']['name']['name'])
        pes = regint.get_by_name(fqn, kind=RegistryInterface.TYPE_PE)
        peid = pes['id']
        already_present = True

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


def clone(name):
    """
    Clone the currently active workspace into a new one named `name`.
    :param name: the name of the new workspace to clone the currently active
    one into.
    """
    try:
        r = regint.clone(name)
        print 'New workspace created: ' + str(r['url'])
    except:
        # See if there is already a workspace with the same name under the
        # current user and notify accordingly
        try:
            w = regint._get_workspace_by_name(regconf.username, name)
            if 'id' in w:
                print 'Workspace ' + name + ' (' + regconf.username + ')' +\
                    ' already exists.'
        except:
            print 'Workspace cloning failed.'


def _short_descr(s, length=30):
    ret = str(s).strip()
    ret = ret.replace('\n', ' ').replace('\r', '')
    if len(ret) > length:
        ret = ret[:length].strip() + ' ...'
    return ret


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
