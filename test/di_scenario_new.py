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

import sys
sys.path.append('resources/')

from eu.verce.util import TimeStamp, Merge

from verce.workflow_graph import WorkflowGraph
from datetime import datetime, timedelta
import uuid

# from verce import registry
# reg = registry.initRegistry()

# from eu.verce.db import MonetDBFileStream
# from eu.verce.seismo import AppendAndSynchronize
# from eu.verce.seismo import Synchro_INGV

requestId=str(uuid.uuid1())
controlParameters = { 'error' : None, 
                      'input' : None, 'inputrootpath': '/data/verce/laquila', 
                      'metadata' : None, 
                      'output' : None, 'outputdest' : '/data/verce/output', 'outputid' : None, 
                      'pid' : None }
                      
from eu.verce.seismo.new.obspy_stream import ObspyStreamPE

def createObspyStreamPE(name, fn):
    streamPE = ObspyStreamPE()
    streamPE.compute_fn = fillGaps
    streamPE.boltId = name
    streamPE.requestId = requestId
    streamPE.controlParameters = controlParameters
    return streamPE
    
def processStream(graph, chain):
    prev = None
    for fn in chain:
        pe = ObspyStreamPE()
        pe.compute_fn = fn
        pe.boltId = name
        pe.requestId = requestId
        pe.controlParameters = controlParameters
        if prev:
            graph.connect(prev, 'output', pe, 'input')
            prev = pe
    return prev, 'output'
    
def createPreprocessing(network, station, channel):

    timestamp = TimeStamp.TimestampGenerator()
    timestamp.starttime = datetime(2011, 5, 1, 0, 0, 0)  # '2011-05-01T00:00:00'
    timestamp.endtime = datetime(2011, 5, 1, 5, 0, 0)    # '2011-05-01T05:00:00'
    timestamp.delta = timedelta(minutes=60)

    parameters = Merge.TupleMerge()
    parameters.repeatedData = [ { 'network':network, 'station':station, 'channel':channel} ]
    parameters.repeatedDataType = ['origin']
    graph.connect(timestamp, 'output', parameters, 'input')

    from eu.verce.seismo.new.MonetDBFileStream import MonetDBFileStream
    files = MonetDBFileStream()
    graph.connect(parameters, 'output', files, 'input')

    from eu.verce.seismo.new.AppendAndSynchronize import AppendAndSynchronize
    appendAndSync = AppendAndSynchronize()
    appendAndSync.boltId = 'AppendAndSynchronize_' + station
    appendAndSync.rootpath = '/data/verce/laquila'
    graph.connect(files, 'output', appendAndSync, 'input')

    from eu.verce.seismo.new.Synchro_INGV import Syncro
    synchro = Syncro()
    synchro.boltId = 'Syncro_' + station
    synchro.requestId = requestId
    synchro.controlParameters = controlParameters
    graph.connect(appendAndSync, 'output', synchro, 'input')
    
    from eu.verce.seismo.new.FillGaps_INGV import fillGaps
    
    fillgaps = ObspyStreamPE()
    fillgaps.compute_fn = fillGaps
    fillgaps.boltId = 'FillGaps_' + station
    fillgaps.requestId = requestId
    fillgaps.controlParameters = controlParameters
    graph.connect(synchro, 'output', fillgaps, 'input')

    return fillgaps, 'output'

if __name__ == '__main__': 
   graph = WorkflowGraph()
   preprocess_AQU, output_name_AQU = createPreprocessing('MN', 'AQU', 'HHZ')
   # preprocess_TERO, output_name_TERO = createPreprocessing('IV', 'TERO', 'HHZ')
   # preprocess_CAMP, output_name_TERO = createPreprocessing('IV', 'CAMP', 'HHZ')

   print "Built workflow"
