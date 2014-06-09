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

from verce import registry
registry.initRegistry()

from eu.verce.test.TestFunction1 import add

if __name__ == '__main__':
   print "2 + 7 = %s" % add(2, 7)

   reg = registry.VerceRegistry()
   reg.user = 1
   reg.group = 1

   functionName = "eu.verce.test.TestFunction345"
   reg.register_function(functionName, 'add', 'eu/verce/test/TestFunction.py')

   reg.delete(functionName)

   peName = "eu.verce.test.TestPE999"
   reg.register_pe(peName, 'MyPE', 'eu/verce/test/MyPE.py')

   from eu.verce.test.TestPE999 import MyPE
   pe = MyPE()
   outputs = pe.process({"in1":1, "in2":2})
   print "Result = %s" % outputs[0]["out1"]

   reg.delete(peName)

