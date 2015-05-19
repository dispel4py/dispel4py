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
import imp


# FIXME: Make this first look things up locally before it goes to the registry
class RegistryImporter(object):
    """A custom module importer to work with the dispel4py Registry."""

    reg_int = None

    def __init__(self, reg_int):
        self.reg_int = reg_int   # the registry interface instance

        # Remove and add the importer to the system meta_path
        sys.meta_path = [i for i in sys.meta_path
                         if type(i) != RegistryImporter]
        if self not in sys.meta_path:
            sys.meta_path.append(self)

    def find_module(self, fullname, path=None):
        workspace_ls = self.reg_int.get_workspace_ls(startswith=fullname)

        # maybe it's a package
        if len(workspace_ls['packages']) > 0:
            return self

        # maybe it's an object?
        try:
            code = self.reg_int.get_code(fullname)
            self.reg_int.registered_entities[fullname] = code
            return self
        except:
            return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]

        mod = imp.new_module(fullname)
        mod.__loader__ = self
        sys.modules[fullname] = mod
        if fullname in self.reg_int.registered_entities:
            code = self.reg_int.registered_entities[fullname]
            # print "compiling code for module " + fullname
            exec code in mod.__dict__
        mod.__file__ = "[%r]" % fullname
        mod.__path__ = []
        return mod
