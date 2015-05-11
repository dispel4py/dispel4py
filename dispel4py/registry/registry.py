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


class Registry(object):
    """An interface to the dispel4py Registry. This interface is intended
    for interactive use, e.g. via the Python interpreter."""

    regint = None
    regconf = None
    regimporter = None

    def __init__(self):
        self.regconf = RegistryConfiguration()
        self.regint = RegistryInterface(self.regconf)
        self.regimporter = RegistryImporter(self.regint)

    def set_def_workspace(self, name, owner=None):
        self.regint.set_workspace(name, owner)
        print ('Default workspaces set to: ' + name + ' (' + owner, ') ' +
               '[' + str(self.regint.workspace) + ']')

    def _prntbln(self, name):
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

    def wls(self, name=None, owner=None, startswith=''):
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
                print ('Cannot resolve workspace for specific user without' +
                       ' a workspace name')
                return
            else:
                wid = self.regint.get_default_workspace_id()
        else:  # a name is provided
            if not owner:
                owner = self.regconf.username
            try:
                wid = self.regint._get_workspace_by_name(owner, name)['id']
            except:
                print ('Cannot resolve workspace with name "' +
                       name + '" owned by "' + owner + '"')
                return

        listing = self.regint.get_workspace_ls(wid, startswith)

        for t in ['pes', 'peimpls', 'functions',
                  'fnimpls', 'literals', 'packages']:
            if len(listing[t]) > 0:
                print self._prntbln(t) + ':'
                for i in listing[t]:
                    if t == 'packages':
                        print ' ', i
                    else:
                        for j in i:
                            print ' ', j, i[j]

    def get_logged_in_username(self):
        print self.regconf.username

    def _reload_module(self, modulestr):
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

    def _clean_gen_docstr(self, s):
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

    def _extract_meta_by_docstr(self, s):
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
        gendoc = self._clean_gen_docstr(s[:fcpos])
        for l in s[fcpos:].split('\n'):  # Assume one doc item by line
            l = l.strip()
            # print l
            if l.startswith(':name'):
                toret['type'].update(self._extract_docstr_name_line(l))
            elif l.startswith(':input'):
                toret['inputs'].append(self._extract_docstr_inout_line(l))
            elif l.startswith(':output'):
                toret['outputs'].append(self._extract_docstr_inout_line(l))
            elif l.startswith(':return'):
                toret['return'].update(self._extract_docstr_return_line(l))
            elif l.startswith(':param'):
                toret['params'].append(self._extract_docstr_param_line(l))
            else:
                pass  # Ignore other lines
        toret['description'] = gendoc
        return toret

    def _extract_docstr_param_line(self, s):
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

    def _extract_docstr_return_line(self, s):
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

    def _extract_docstr_name_line(self, s):
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
    def _extract_docstr_inout_line(self, s):
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

    def _find_earliest_doc_kw(self, s, beg=0):
        """
        Find the earliest instance of a keyword in the given string and
        return its position. This uses the `find` method of str.
        :param s: the string to be scanned
        :return the earliest position of a match, -1 otherwise
        """
        kws = [':name', ':input', ':output']
        pos = [s.find(k) for k in kws]
        return min(pos)

    # TODO: Add registration policy in case of name clash (replace, etc.)
    def register_fn(self, name, impl_subpackage='_impls'):
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
        m = self._reload_module(module)
        code = inspect.getsource(m)
        fn = getattr(m, fnname)
        doc = fn.__doc__.strip()

        meta = self._extract_meta_by_docstr(doc)

        # import pprint
        # pp = pprint.PrettyPrinter(indent=2)
        # pp.pprint(meta)
        # print '--CODE:' + '-' * 70
        # print code
        # print '-------' + '-' * 70

        try:
            fns = self.regint.register_fn_spec(meta['type']['name']['pckg'],
                                               meta['type']['name']['name'],
                                               meta['return']['type'],
                                               descr=meta['description'])

            # The parameters:
            fnid = fns['id']
            for p in meta['params']:
                self.regint.add_fn_param(p['name'], str(fnid), p['type'])
        except:
            # The function could already be registered, so retrieve it
            fqn = (meta['type']['name']['pckg'] + '.' +
                   meta['type']['name']['name'])
            fns = self.regint.get_by_name(fqn, kind=RegistryInterface.TYPE_FN)
            fnid = fns['id']

        # Register the implementation and associate it to the spec above
        try:
            self.regint.add_fn_implementation(
                str(fnid),
                code,
                pckg=meta['type']['name']['pckg'] + '.' + impl_subpackage,
                name=meta['type']['name']['name'],
                description=meta['description'])
        except:
            pass

        self.regint.delete_fnspec(str(fnid))

    # TODO: Add registration policy in case of name clash
    def register_pe(self, name, impl_subpackage='_impls'):
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
        m = self._reload_module(module)
        pe = getattr(m, pename)
        doc = pe.__doc__.strip()

        meta = self._extract_meta_by_docstr(doc)

        # import pprint
        # pp = pprint.PrettyPrinter(indent=2)
        # pp.pprint(meta)

        # Actual registrations via the core module here:
        try:
            # The spec:
            pes = self.regint.register_pe_spec(meta['type']['name']['pckg'],
                                               meta['type']['name']['name'],
                                               descr=meta['description'])
            # The connections:
            peid = pes['id']
            for c in meta['inputs'] + meta['outputs']:
                self.regint.add_pe_connection(
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
            pes = self.regint.get_by_name(fqn, kind=RegistryInterface.TYPE_PE)
            peid = pes['id']

        # The implementation:
        code = inspect.getsource(m)
        try:
            self.regint.add_pe_implementation(
                str(peid),
                code=code,
                pckg=meta['type']['name']['pckg'] + '.' + impl_subpackage,
                name=meta['type']['name']['name'],
                description=meta['description'])
        except:
            # Do not do anything if a PEImpl of same fqn is already present
            pass

        # import os
        # os.system('read')
        #
        # self.regint.delete_pespec(str(peid))

# def init():
#     """Initialisation for interactive uses. It makes use of the default
#     configuration mechanism."""
#     conf = RegistryConfiguration()
#
#     reg = RegistryInterface()
#     reg.protocol, reg.host, reg.port = split_url(conf.url)
#     reg.user = conf.username
#     if not conf.def_workspace:
#         wspc_name = DEF_WSPC_NAME
#     else:
#         wspc_name = conf.def_workspace
#
#     reg.login(conf.username, conf.password)
#     wspc_id = reg._get_workspace_by_name(conf.username, wspc_name)['id']
#     reg.workspace = wspc_id
#
#     sys.meta_path.append(reg)
#     return reg


def main():
    # print 'Registry main'
    r = Registry()
    # r._reload_module('dispel4py.registry.registry.Registry')

    # print r._extract_docstr_inout_line(' :input myin: <str> some first' +
    #                                    ' descr')
    # print r._extract_docstr_inout_line(' :output myout: [str] <words> some' +
    #                                    ' 2nd description')
    # print r._extract_docstr_inout_line(' :output myout2:  some 3rd' +
    #                                    ' description')

    # r.register_pe("tests.rand_word.RandomWordProducer")
    # r.register_pe("tests.test_pe.RandomTestPE")

    r.register_fn('tests.testfn.myfn')

if __name__ == '__main__':
    main()
