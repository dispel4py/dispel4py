// Copyright (c) The University of Edinburgh 2014
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dispel4py.storm;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TIOStreamTransport;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class ThriftSubmit 
{

    public static void main(String[] args) throws Exception
    {
        if (args.length < 2)
        {
            System.out.println("Usage: ThriftSubmit <thriftfile> <topologyname>");
            return;
        }
        String thriftFile = args[0];
        String topologyName = args[1];
        StormTopology topology = readThrift(thriftFile);
        final LocalCluster local = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);
        local.submitTopology(topologyName, config, topology);
    }
        
    private static StormTopology readThrift(String fileName) 
            throws FileNotFoundException, TException
    {
        StormTopology topology = new StormTopology();
        InputStream bufferedIn = new BufferedInputStream(new FileInputStream(fileName), 2048);
        TBinaryProtocol iprot = new TBinaryProtocol(new TIOStreamTransport(bufferedIn));
        topology.read(iprot);
        return topology;
    }


}
