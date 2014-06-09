# Copyright (c) The University of Edinburgh 2014
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#	 Unless required by applicable law or agreed to in writing, software
#	 distributed under the License is distributed on an "AS IS" BASIS,
#	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	 See the License for the specific language governing permissions and
#	 limitations under the License.

"""
Use as follows:
$ storm-0.8.2/bin/storm shell resources/ python storm-test.py
"""

import sys
import time
import json

from storm import ttypes as tt
from verce.storm_topology import StormClient

if __name__ == '__main__':
   print sys.argv
   host = sys.argv[1]
   port = sys.argv[2]
   uploaded_jar_location = sys.argv[3]

   word_gen_conf={'verce.module' : 'WordGeneratorPE', 'verce.script' : 'RandomWordProducer'}
   word_gen = tt.SpoutSpec(spout_object=tt.ComponentObject(shell=tt.ShellComponent("python", "source_wrapper.py")), 
                           common=tt.ComponentCommon(inputs={}, 
                                                     streams= { 'output' : tt.StreamInfo(['id', 'stuff'], False) },
                                                     json_conf=json.dumps(word_gen_conf)))
   random_filter_conf={'verce.module' : 'RandomFilterPE', 
                       'verce.script' : 'RandomFilter', 
                       'verce.inputmapping' : { 'word_gen' : { 'output' : 'input'} } }
   random_filter = tt.Bolt(bolt_object=tt.ComponentObject(shell=tt.ShellComponent("python", "simple_wrapper.py")), 
                           common=tt.ComponentCommon(inputs={ tt.GlobalStreamId('word_gen', 'output') : tt.Grouping(shuffle=tt.NullStruct()) }, 
                                                     streams= { 'output' : tt.StreamInfo(['id', 'stuff'], False) },
                                                     json_conf=json.dumps(random_filter_conf)))

   topology = tt.StormTopology(spouts={ "word_gen" : word_gen }, bolts={ "random_filter" : random_filter }, state_spouts={})

   topology_name = "WrapperTest"
   conf = { 'topology.workers':3 }
   client = StormClient(host, port)
   try:
       print "Submitting topology '%s' to %s:%s ... " % (topology_name, host, port)    
       client.submitTopology(topology_name, uploaded_jar_location, conf, topology)
   except tt.AlreadyAliveException:
       print "Submission failed: Topology '%s' already exists." % topology_name

   # client.killTopology(topology_name)
   # print "Killed topology '%s'" % topology_name
