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

from verce.workflow_graph import WorkflowGraph
from datetime import datetime, timedelta
import uuid

import sys
sys.path.append('resources/')

from eu.verce.util import TimeStamp, Merge
from eu.verce.seismo.new.MonetDBFileStream import MonetDBFileStream
from eu.verce.seismo.new.AppendAndSynchronize import AppendAndSynchronize
    
from verce import registry
reg = registry.initRegistry()

from eu.verce.seismo.new.Synchro_INGV import synchro
# from eu.verce.seismo.new.Synchro_INGV_fn import synchro
from eu.verce.seismo.new.FillGaps_INGV import fillGaps
from eu.verce.seismo.new.Detrend_CM import detrend
from eu.verce.seismo.new.Whiten_INGV import whiten
from eu.verce.seismo.new.Temp_Normalization_C import tempNormalize

requestId=str(uuid.uuid1())
controlParameters = { 'error' : None, 
                      'input' : None, 'inputrootpath': '/data/verce/laquila', 
                      'metadata' : None, 
                      'output' : None, 'outputdest' : '/data/verce/output', 'outputid' : None, 
                      'pid' : None }
                      
from eu.verce.seismo.new.obspy_process import createProcessingComposite
from eu.verce.seismo.provenance import ProvenanceRecorder

provRecorder = ProvenanceRecorder()
provRecorder.targetURL = 'http://129.215.213.249:8082/'

def createPreprocessing(network, station, channel):

    timestamp = TimeStamp.TimestampGenerator()
    timestamp.starttime = datetime(2011, 5, 1, 0, 0, 0)  # '2011-05-01T00:00:00'
    timestamp.endtime = datetime(2011, 5, 1, 5, 0, 0)    # '2011-05-01T05:00:00'
    timestamp.delta = timedelta(minutes=60)
    
    parameters = Merge.TupleMerge()
    parameters.repeatedData = [ { 'network':network, 'station':station, 'channel':channel} ]
    parameters.repeatedDataType = ['origin']
    graph.connect(timestamp, 'output', parameters, 'input')
    
    files = MonetDBFileStream()
    graph.connect(parameters, 'output', files, 'input')
    
    appendAndSync = AppendAndSynchronize()
    appendAndSync.boltId = 'AppendAndSynchronize_' + station
    appendAndSync.rootpath = '/data/verce/laquila'
    graph.connect(files, 'output', appendAndSync, 'input')
        
    processingChain = []
    processingChain.append(synchro)
    processingChain.append((fillGaps, { 'pergap' : 20, 'unitl' : 3600 }))
    processingChain.append((detrend, { 'method' : 'linear' }))
    processingChain.append((detrend, { 'method' : 'demean' }))
    processingChain.append((whiten, { 'flo' : 0.01, 'fhi' : 1.0 }))
    processingChain.append((tempNormalize, { 'clip_factor' : 1.5, 'clip_weight' : 2.0, 'norm_win' : 2, 'norm_method' : '1bit' }))
    
    composite = createProcessingComposite(processingChain, '_' + station, requestId, controlParameters, provRecorder)
    graph.connect(appendAndSync, 'output', composite, 'input')
    
    return composite, 'output'
  
if __name__ == '__main__':  
   graph = WorkflowGraph()
   preprocess_AQU, output_name_AQU = createPreprocessing('MN', 'AQU', 'HHZ')
   # preprocess_TERO, output_name_TERO = createPreprocessing('IV', 'TERO', 'HHZ')
   # preprocess_CAMP, output_name_TERO = createPreprocessing('IV', 'CAMP', 'HHZ')

   print "Built workflow"
