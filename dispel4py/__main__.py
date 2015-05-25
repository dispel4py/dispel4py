# Copyright (c) The University of Edinburgh 2014-2015
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
from importlib import import_module


def main(args=None):

    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph for processing.')
    parser.add_argument('target', help='target execution platform')
    args, remaining = parser.parse_known_args()
    try:
        from dispel4py.new import mappings
        # see if platform is in the mappings file as a simple name
        target = mappings.config[args.target]
    except KeyError:
        # it is a proper module name - fingers crossed...
        target = args.target
    try:
        process = getattr(import_module(target), 'main')
    except:
        # print traceback.format_exc()
        print 'Unknown target: %s' % target
        return
    process()


if __name__ == "__main__":
    main()
