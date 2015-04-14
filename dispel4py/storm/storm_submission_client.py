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

sys.path.append('resources')

from dispel4py.storm.client import StormClient

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from storm import ttypes as tt


if __name__ == '__main__':

    with open("topology.thrift", "r") as thrift_file:
        bytes = thrift_file.read()

    transportIn = TTransport.TMemoryBuffer(bytes)
    protocolIn = TBinaryProtocol.TBinaryProtocol(transportIn)
    topology = tt.StormTopology()
    topology.read(protocolIn)

    topology_name, host, port, uploaded_jar_location = sys.argv[1:]

    conf = {'topology.workers': 10, 'topology.max.spout.pending': 20}
    client = StormClient(host, port)
    try:
        print "Submitting topology '%s' to %s:%s ... "\
            % (topology_name, host, port)
        client.submitTopology(topology_name,
                              uploaded_jar_location,
                              conf,
                              topology)
    except tt.AlreadyAliveException:
        print "Submission failed: Topology '%s' already exists."\
            % topology_name
