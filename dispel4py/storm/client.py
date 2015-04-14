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

import json

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from storm import Nimbus


class StormClient:

    '''
    A simple Storm client that connects to the cluster at 'host':'port'.
    '''

    def __init__(self, host, port):
        self.host = host
        self.port = port
        transport = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Nimbus.Client(self.protocol)

    def submitTopology(self, name, uploaded_jar_location, conf, topology):
        '''
        Submits the specified topology with name 'name' to the configured host.

        :param name: name given to the topology
        :param uploaded_jar_location: the path to the location of the jar as \
        created by Storm
        :param conf: configuration of the topology
        :param topology: a Storm topology
        '''
        self.transport.open()
        try:
            self.client.submitTopology(name, uploaded_jar_location,
                                       json.dumps(conf), topology)
        finally:
            self.transport.close()

    def killTopology(self, name):
        '''
        Kills the topology identified by 'name'.
        '''
        self.transport.open()
        try:
            self.client.killTopology(name)
        finally:
            self.transport.close()
