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

from eu.verce.util import TimeStamp, Merge, FileNameGenerator
from eu.verce.seismo import VerceSeismo, VerceSeismoLockstep

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

def createPreprocessing(network, station, channel):

    timestamp = TimeStamp.TimestampGenerator()
    timestamp.starttime = datetime(2011, 5, 1, 0, 0, 0)  # '2011-05-01T00:00:00'
    timestamp.endtime = datetime(2011, 5, 1, 5, 0, 0)    # '2011-05-01T05:00:00'
    timestamp.delta = timedelta(minutes=60)

    parameters = Merge.TupleMerge()
    parameters.repeatedData = [ { 'network':network, 'station':station, 'channel':channel} ]
    parameters.repeatedDataType = ['origin']
    graph.connect(timestamp, 'output', parameters, 'input')

    # temporary - hard coded file names for testing 
    # files = FileNameGenerator.FileNameGenerator()
    from eu.verce.seismo.new.MonetDBFileStream import MonetDBFileStream
    files = MonetDBFileStream()
    graph.connect(parameters, 'output', files, 'input')

    from eu.verce.seismo.new.AppendAndSynchronize import AppendAndSynchronize
    appendAndSync = AppendAndSynchronize()
    appendAndSync.boltId = 'AppendAndSynchronize_' + station
    appendAndSync.rootpath = '/data/verce/laquila'

    graph.connect(files, 'output', appendAndSync, 'input')

    synchro = VerceSeismo.SeismoPE()
    synchro.modname = 'eu.verce.seismo.Synchro_INGV'
    synchro.scriptname = 'Syncro'
    synchro.controlParameters = controlParameters
    synchro.boltId = 'Syncro_' + station
    synchro.requestId = requestId

    graph.connect(appendAndSync, 'output', synchro, 'input')

    fillgaps = VerceSeismo.SeismoPE()
    fillgaps.modname = 'eu.verce.seismo.FillGaps_INGV'
    fillgaps.scriptname = 'FillGaps'
    fillgaps.controlParameters = controlParameters
    fillgaps.appParameters = { 'pergap' : 20, 'unitl' : 3600 }
    fillgaps.boltId = 'FillGaps_' + station
    fillgaps.requestId = requestId

    graph.connect(synchro, 'output', fillgaps, 'input')

    detrendLinear = VerceSeismo.SeismoPE()
    detrendLinear.modname = 'eu.verce.seismo.Detrend_CM'
    detrendLinear.scriptname = 'Detrend'
    detrendLinear.controlParameters = controlParameters
    detrendLinear.appParameters = { 'method' : 'linear' }
    detrendLinear.boltId = 'DetrendLinear_' + station
    detrendLinear.requestId = requestId

    graph.connect(fillgaps, 'output', detrendLinear, 'input')

    detrendDemean = VerceSeismo.SeismoPE()
    detrendDemean.modname = 'eu.verce.seismo.Detrend_CM'
    detrendDemean.scriptname = 'Detrend'
    detrendDemean.controlParameters = controlParameters
    detrendDemean.appParameters = { 'method' : 'demean' }
    detrendDemean.boltId = 'DetrendDemean_' + station
    detrendDemean.requestId = requestId

    graph.connect(detrendLinear, 'output', detrendDemean, 'input')

    whiten = VerceSeismo.SeismoPE()
    whiten.modname = 'eu.verce.seismo.Whiten_INGV'
    whiten.scriptname = 'Whiten_INGV'
    whiten.controlParameters = controlParameters
    whiten.appParameters = { 'flo' : 0.01, 'fhi' : 1.0 }
    whiten.boltId = 'Whiten_' + station
    whiten.requestId = requestId

    graph.connect(detrendDemean, 'output', whiten, 'input')

    tempNorm = VerceSeismo.SeismoPE()
    tempNorm.modname = 'eu.verce.seismo.Temp_Normalization_C'
    tempNorm.scriptname = 'TempNormalization'
    tempNorm.controlParameters = controlParameters
    tempNorm.appParameters = { 'clip_factor' : 1.5, 'clip_weight' : 2.0, 'norm_win' : 2, 'norm_method' : '1bit' }
    tempNorm.boltId = 'TempNormalization_' + station
    tempNorm.requestId = requestId

    graph.connect(whiten, 'output', tempNorm, 'input')
    
    return tempNorm, 'output'


if __name__ == '__main__':
   graph = WorkflowGraph()

   preprocess_AQU, output_name_AQU = createPreprocessing('MN', 'AQU', 'HHZ')
   preprocess_TERO, output_name_TERO = createPreprocessing('IV', 'TERO', 'HHZ')
   preprocess_CAMP, output_name_CAMP = createPreprocessing('IV', 'CAMP', 'HHZ')

   xcor_AQU_TERO = VerceSeismoLockstep.LockstepSeismoPE()
   xcor_AQU_TERO.addInput('input1', grouping=['timestamp'])
   xcor_AQU_TERO.addInput('input2', grouping=['timestamp'])
   xcor_AQU_TERO.modname = 'eu.verce.seismo.CorrelateNoise_CM'
   xcor_AQU_TERO.scriptname = 'CorrelateNoise'
   xcor_AQU_TERO.controlParameters = controlParameters
   xcor_AQU_TERO.appParameters = { 'timeshift' : 20000 }
   xcor_AQU_TERO.boltId = 'CrossCorrelation_AQU_TERO'
   xcor_AQU_TERO.requestId = requestId

   graph.connect(preprocess_AQU, output_name_AQU, xcor_AQU_TERO, 'input1')
   graph.connect(preprocess_TERO, output_name_TERO, xcor_AQU_TERO, 'input2')
   # 
   # tempNorm_AQU_TERO = VerceSeismo.SeismoPE()
   # tempNorm_AQU_TERO.inputconnections['input']['grouping']=['origin']
   # tempNorm_AQU_TERO.modname = 'eu.verce.seismo.ArrayStacking'
   # tempNorm_AQU_TERO.scriptname = 'ArrayStacking'
   # tempNorm_AQU_TERO.controlParameters = controlParameters
   # tempNorm_AQU_TERO.appParameters = { 'stackingid' : requestId + '-AQU_TERO' }
   # tempNorm_AQU_TERO.boltId = 'ArrayStacking_AQU_TERO'
   # tempNorm_AQU_TERO.requestId = requestId
   # 
   # graph.connect(xcor_AQU_TERO, 'output', tempNorm_AQU_TERO, 'input')
   # 


   print "Built workflow"
