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

from dispel4py import registry
import os 
import traceback

if __name__ == '__main__':
   reg = registry.VerceRegistry()
   reg.workspace = 1
   if 'VERCEREGISTRY_HOST' in os.environ:
       reg.registry_url = os.environ['VERCEREGISTRY_HOST'].encode('utf-8') + '/VerceRegistry/rest/'
   reg.login('admin', 'admin')
   
   functionName = "dispel4py.test.TestFunction1"
   try:
       reg.register_function(functionName, 'add', 'test/TestFunction.py')
   except:
       print traceback.format_exc()
       print "Couldn't register " + functionName
    
   try:
       reg.register_pe('dispel4py.Filter', 'RandomFilter', 'test/TestPEs.py')
   except: 
       print "Couldn't register dispel4py.test.RandomFilter"
    
   try:
       reg.register_pe('dispel4py.test.RandomWordProducer', 'RandomWordProducer', 'test/TestPEs.py')
   except:
       print "Couldn't register dispel4py.test.RandomWordProducer"

   # now import and test the function
   registry.initRegistry('admin', 'admin', url=reg.registry_url)
   from dispel4py.test.TestFunction1 import add

   print "2 + 7 = %s" % add(2, 7)

