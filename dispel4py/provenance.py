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

from dispel4py.core import GenericPE, NAME, TYPE
import datetime
import uuid
import traceback
import os
import socket

INPUT_NAME = 'input'
OUTPUT_DATA = 'output'
OUTPUT_METADATA = 'metadata'

class ProvenancePE(GenericPE):
    
    def __init__(self):
        GenericPE.__init__(self)
        in1 = {}
        in1[NAME] = INPUT_NAME
        self.inputconnections[INPUT_NAME] = in1
        out1 = {}
        out1[TYPE] = ['streams']
        out1[NAME] = OUTPUT_DATA
        out_md = {}
        out_md[NAME] = OUTPUT_METADATA
        out_md[TYPE] = ['metadata']
        self.outputconnections[OUTPUT_DATA] = out1
        self.outputconnections[OUTPUT_METADATA] = out_md
        self.taskId=str(uuid.uuid1())
        self.controlParameters = {}
        self.appParameters = {}
        self.provon = True
        self.iterationIndex = 0
        self.instanceId = 'Invoker-instance-'+socket.gethostname()
        
    def __getUniqueId(self):
        return socket.gethostname()+"-"+str(os.getpid())+"-"+str(uuid.uuid1())

    def setOutputDataTuple(self, types):
        self.outputconnections[OUTPUT_DATA][TYPE] = types

    def getDataStreams(self, inputs):
        '''
        This method expects the input tuple to have a single element which contains the data.
        Override this method to create input streams with a different input type, for example
        if the input tuple contains other elements.
        '''
        data = inputs[INPUT_NAME]
        streams = {"streams": data}
        return streams

    def writeOutputStreams(self, outputs):
        output_data = []
        for v in outputs["streams"]:
            output_data.append(v)
        result = { OUTPUT_DATA : output_data }
        return result

    def process(self, inputs):
        streams = self.getDataStreams(inputs)
        # processing...
        self.iterationIndex += 1
        output = self.__processwrapper(streams)
        result = self.writeOutputStreams(output)
        # copy the metadata to another output stream so it can be collected separately.   
        if result is None or output is None or OUTPUT_METADATA not in output:
            # if there's no metadata then that's fine
            pass
        else:
            if self.provon:
                result[OUTPUT_METADATA] = output['metadata']
        return result
                    
    def __processwrapper(self, streams):
        try:
            self.initParameters(streams)
            self.importInputData()
            self.__importInputMetadata()
            self.__computewrapper()
            self.__writeOutputwrapper()
        except:
            self.log(traceback.format_exc())
            self.__getMetadataWrapper()
            output={"streams":[{"data":None}],"metadata":self.metadata,"error":self.error,"pid":"%s" % (os.getpid(),)}
            
        return self.packageAll()
        
    def initParameters(self, streams):
        
        self.error=''
        self.input = streams
        self.verce = self.controlParameters
        self.parameters = self.appParameters

        self.metadata={};
        self.annotations={};
        self.stateful=False;
        self.inMetaStreams=None
         
        if self.provon==True:
            try:
                self.iterationId = self.name + '-' + self.__getUniqueId()
                self.runId=self.verce["runId"]
                self.username=self.verce["username"]
            except:
                pass
        
        self.outputstreams=list()
        self.outputattr=list()
        self.output=list()
        self.streams=list()
        self.derivationIds=list()
        self.streamItemsLocations=list()
        self.streamItemsFormat=list()
        self.streamItemsAnnotations=list()
        self.streamItemsPorts=list()
        self.outputdest=self.verce['outputdest'] if 'outputdest' in self.verce else 'None'
        self.rootpath=self.verce['inputrootpath'] if 'inputrootpath' in self.verce else 'None'
        self.outputid=self.verce['outputid'] if 'outputid' in self.verce else 'None'
            
    def importInputData(self):
        try:
            self.streams=list()
            self.attributes = list()
            while (self.input["streams"]):
                streamItem=self.input["streams"].pop(0)
                self.streams.append(streamItem["data"])
                try:
                    self.attributes.append(streamItem['attr'])
                except KeyError:
                    self.attributes.append(None)
                    
                if self.provon:
                    self.buildDerivation(streamItem)

        except Exception:
            self.output="";
            self.error+= "Reading Input Error: %s" % traceback.format_exc()
            raise
                        
    def __computewrapper(self):
        try:
            if len(self.streams)==1:
                self.st=self.streams[0]
            if len(self.attributes)==1:
                self.attr=self.attributes[0]

            self.startTime=datetime.datetime.now()

            try:
                self.compute()     
            except Exception:
                self.log(traceback.format_exc())
                self.error+=" Compute Error: %s" % traceback.format_exc()

            self.endTime=datetime.datetime.now()
        finally:
            self.__getMetadataWrapper()
    
    def compute(self):
        ''' To be implemented '''
        return None
            
    def __writeOutputwrapper(self):
        try:
            self.writeOutput()
        except Exception:
            self.error+=self.name+" Writing output Error: %s" % traceback.format_exc()
            raise
    
    def writeOutput(self):
        ''' To be overridden '''
        try:
            if (len(self.outputstreams)==0):
                self.outputstreams=self.streams
            if not self.outputattr:
                self.outputattr=self.attributes
            
            for st, attr in zip(self.outputstreams, self.outputattr):
                
                streamtransfer={}
                streamtransfer['data'] = st
                streamtransfer['attr'] = attr

                if self.provon:
                    streamtransfer['id'] = ''
                    streamtransfer.update({"TriggeredByProcessIterationID":self.iterationId})
                self.output.append(streamtransfer)
            
        except Exception, err:
            self.error+=self.name+" Writing output Error: %s" % traceback.format_exc()
            raise

    def packageAll(self):
        if self.provon:
            try:
                self.metadata.update({'_id':self.iterationId})
                self.metadata.update({'iterationIndex':self.iterationIndex})
                self.metadata.update({'instanceId':self.instanceId})
                self.metadata.update({'annotations':self.dicToKeyVal(self.annotations,True)})
                self.metadata.update({'stateful':self.stateful})
                self.metadata.update({'site':socket.gethostname()})
                self.metadata.update({'parameters':self.dicToKeyVal(self.parameters)})
                self.metadata.update({'errors':self.error})
                self.metadata.update({'pid':'%s' % (os.getpid())})
                self.metadata.update({'derivationIds':self.derivationIds})
                self.metadata.update({'name':self.name})
                self.metadata.update({'runId':self.runId})
                self.metadata.update({'username':self.username})
                self.metadata.update({'startTime':str(self.startTime)})
                self.metadata.update({'endTime':str(self.endTime)})
                self.metadata.update({'type':'lineage'})
            except Exception:
                self.error+=" Packaging Error: %s" % traceback.format_exc()
                    
        output={"streams": self.output,"metadata":self.metadata,"error":self.error,"pid":"%s" % (os.getpid(),)}
        return output
            

    """
    Imports Input metadata if available, the metadata will be available in the self.inMetaStreams property as a Dictionary"
    """
    def __importInputMetadata(self):
        try:
            self.inMetaStreams=self.input["metadata"]["streams"];
        except Exception,err:
            None
    """ 
    Reads and formats the stream's metadata
    """
    def __getMetadataWrapper(self):
        try:
            if (len(self.outputstreams)==0):
                self.outputstreams=self.streams
            if self.provon==True:
                self.metadata.update({"streams":self.getMetadata()})
        except Exception, err:
                streamlist=list()
                streamItem={}
                streammeta=list()
                streamItem.update({"content": streammeta})
                streamItem.update({"id":self.__getUniqueId()});
                streamlist.append(streamItem)
                self.metadata.update({"streams":streamlist});
                self.error+=self.name+" Metadata extraction Error: %s" % traceback.format_exc()

    def getMetadata(self):
        streamlist=list()
        for st in self.outputstreams:
            streamItem={}
            streammeta={}
            streammeta=self.extractItemMetadata(st);
            if type(streammeta) != list:
                streamItem.update({"content": [streammeta]})
            else:
                streamItem.update({"content": streammeta})
            streamItem.update({"id":self.__getUniqueId()});
            streamItem.update({"format":""})
            streamItem.update({"location":""})
            streamItem.update({"annotations":[]})
            if (len(self.streamItemsPorts)!=0):
                streamItem.update({"port": self.streamItemsPorts.pop(0)})
            if (len(self.streamItemsLocations)!=0):
                streamItem.update({"location": self.streamItemsLocations.pop(0)})
            if (len(self.streamItemsFormat)!=0):
                streamItem.update({"format": self.streamItemsFormat.pop(0)})
            if (len(self.streamItemsAnnotations)!=0):
                streamItem.update({"annotations": self.dicToKeyVal(self.streamItemsAnnotations.pop(0),True)})
            streamlist.append(streamItem)
        return streamlist

    def buildDerivation(self,data):
        try:
            derivation={'DerivedFromDatasetID':data['id'],'TriggeredByProcessIterationID':data['TriggeredByProcessIterationID']}
            self.derivationIds.append(derivation)
          
        except Exception:
            if self.provon:
                self.error+= " Build Derivation Error: %s" % traceback.format_exc()

    def dicToKeyVal(self,dict,valueToString=False):
        try:
            alist=list()
            for k, v in dict.iteritems():
                adic={}
                adic.update({"key":str(k)})
                if valueToString:
                    adic.update({"val":str(v)})
                else:
                    adic.update({"val":v})
                    
                alist.append(adic)
            return alist
        except Exception:
            self.error+= " dicToKeyVal output Error: %s" % traceback.format_exc()
