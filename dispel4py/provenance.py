#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import dispel4py.new.processor
from dispel4py.utils import make_hash
from dispel4py.core import GenericPE
from dispel4py.base import IterativePE, NAME, SimpleFunctionPE
from dispel4py.workflow_graph import WorkflowGraph
import sys
import datetime
import uuid
import traceback
import os
import socket
import json
import httplib
import urllib
from urlparse import urlparse
from dispel4py.new import simple_process
from subprocess import Popen, PIPE
import collections

from itertools import chain
try:
    from reprlib import repr
except ImportError:
    pass

INPUT_NAME = 'input'
OUTPUT_DATA = 'output'
OUTPUT_METADATA = 'provenance'


#def write(self, name, data, **kwargs):
#    self._write(name, data)
    


def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    #deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = sys.getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        #if verbose:
        #    print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def write(self, name, data):
    if isinstance(data, dict) and '_d4p_prov' in data:
#            meta = data['_d4p_prov']
            data = (data['_d4p_data'])
    self._write(name,data)

#dispel4py.core.GenericPE.write = write
dispel4py.base.SimpleFunctionPE.write = write


def getDestination_prov(self, data):
    print ("GGGGGGGGG "+str(data[self.input_name]))
    if 'TriggeredByProcessIterationID' in data[self.input_name]:
        output = tuple([data[self.input_name]['_d4p'][x]
                        for x in self.groupby])
    else:
        output = tuple([data[self.input_name][x] for x in self.groupby])
    dest_index = abs(make_hash(output)) % len(self.destinations)
    return [self.destinations[dest_index]]


def commandChain(commands, envhpc, queue=None):

    for cmd in commands:
        print('Executing commandChain:' + str(cmd))
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, env=envhpc, shell=True)
        stdoutdata, stderrdata = process.communicate()

    if queue is not None:
        queue.put([stdoutdata, stderrdata])
        queue.close()
    else:
        return stdoutdata, stderrdata


def toW3Cprov(prov, format='w3c-prov-json'):
    from dispel4py.prov.model import ProvDocument
    from dispel4py.prov.model import Namespace
    from dispel4py.prov.model import PROV

    g = ProvDocument()
    # namespaces do not need to be explicitly added to a document
    vc = Namespace("verce", "http://verce.eu")
    g.add_namespace("dcterms", "http://purl.org/dc/terms/")

    'specifing user'
    # first time the ex namespace was used, it is added to the document
    # automatically
    g.agent(vc["ag_" + prov["username"]],
            other_attributes={"dcterms:author": prov["username"]})

    'specify bundle'

    if prov['type'] == 'workflow_run':

        prov.update({'runId': prov['_id']})
        dic = {}
        i = 0

        for key in prov:

            if key != "input":
                if ':' in key:
                    dic.update({key: prov[key]})
                else:
                    dic.update({vc[key]: prov[key]})

        dic.update({'prov:type': PROV['Bundle']})
        g.entity(vc[prov["runId"]], dic)
    else:
        g.entity(vc[prov["runId"]], {'prov:type': PROV['Bundle']})

    g.wasAttributedTo(vc[prov["runId"]],
                      vc["ag_" + prov["username"]],
                      identifier=vc["run_" + prov["runId"]])
    bundle = g.bundle(vc[prov["runId"]])

    'specifing creator of the activity (to be collected from the registy)'

    if 'creator' in prov:
        # first time the ex namespace was used, it is added to the document
        # automatically
        bundle.agent(vc["ag_" + prov["creator"]],
                     other_attributes={"dcterms:creator": prov["creator"]})
        bundle.wasAssociatedWith(
            'process_' + prov["_id"], vc["ag_" + prov["creator"]])
        bundle.wasAttributedTo(vc[prov["runId"]], vc["ag_" + prov["creator"]])

    ' check for workflow input entities'
    if prov['type'] == 'workflow_run':
        dic = {}
        i = 0
        if not isinstance(prov['input'], list):
            prov['input'] = [prov['input']]
            for y in prov['input']:
                for key in y:
                    if ':' in key:
                        dic.update({key: y[key]})
                    else:
                        dic.update({vc[key]: y[key]})
            dic.update({'prov:type': 'worklfow_input'})
            bundle.entity(vc["data_" + prov["_id"] + "_" + str(i)], dic)
            bundle.wasGeneratedBy(vc["data_" +
                                     prov["_id"] +
                                     "_" +
                                     str(i)], identifier=vc["wgb_" +
                                                            prov["_id"] +
                                                            "_" +
                                                            str(i)])

            i = i + 1
        if format == 'w3c-prov-xml':
            return str(g.serialize(format='xml'))
        else:
            return json.loads(g.serialize(indent=4))

    'adding activity information for lineage'
    dic = {}
    for key in prov:

        if not isinstance(prov[key], list):
            if ':' in key:
                dic.update({key: prov[key]})
            else:
                if key == 'location':

                    dic.update({"prov:location": prov[key]})
                else:
                    dic.update({vc[key]: prov[key]})

    bundle.activity(vc["process_" + prov["_id"]],
                    prov["startTime"],
                    prov["endTime"],
                    dic.update({'prov:type': prov["name"]}))

    'adding parameters to the document as input entities'
    dic = {}
    for x in prov["parameters"]:
        if ':' in x["key"]:
            dic.update({x["key"]: x["val"]})
        else:
            dic.update({vc[x["key"]]: x["val"]})

    dic.update({'prov:type': 'parameters'})
    bundle.entity(vc["parameters_" + prov["_id"]], dic)
    bundle.used(vc['process_' + prov["_id"]],
                vc["parameters_" + prov["_id"]],
                identifier=vc["used_" + prov["_id"]])

    'adding entities to the document as output metadata'
    for x in prov["streams"]:
        i = 0
        parent_dic = {}
        for key in x:

            if key == 'location':

                parent_dic.update({"prov:location": str(x[key])})
            else:
                parent_dic.update({vc[key]: str(x[key])})

        c1 = bundle.collection(vc[x["id"]], other_attributes=parent_dic)
        bundle.wasGeneratedBy(vc[x["id"]],
                              vc["process_" + prov["_id"]],
                              identifier=vc["wgb_" + x["id"]])

        for d in prov['derivationIds']:
            bundle.wasDerivedFrom(vc[x["id"]],
                                  vc[d['DerivedFromDatasetID']],
                                  identifier=vc["wdf_" + x["id"]])

        for y in x["content"]:

            dic = {}

            if isinstance(y, dict):
                val = None
                for key in y:

                    try:
                        val = num(y[key])

                    except Exception:
                        val = str(y[key])

                    if ':' in key:
                        dic.update({key: val})
                    else:
                        dic.update({vc[key]: val})
            else:
                dic = {vc['text']: y}

            dic.update({"verce:parent_entity": vc["data_" + x["id"]]})
            e1 = bundle.entity(vc["data_" + x["id"] + "_" + str(i)], dic)

            bundle.hadMember(c1, e1)
            bundle.wasGeneratedBy(vc["data_" +
                                     x["id"] +
                                     "_" +
                                     str(i)],
                                  vc["process_" +
                                     prov["_id"]],
                                  identifier=vc["wgb_" +
                                                x["id"] +
                                                "_" +
                                                str(i)])

            for d in prov['derivationIds']:
                bundle.wasDerivedFrom(vc["data_" + x["id"] + "_" + str(i)],
                                      vc[d['DerivedFromDatasetID']],
                                      identifier=vc["wdf_" + "data_" +
                                                    x["id"] + "_" + str(i)])

            i = i + 1

    if format == 'w3c-prov-xml':
        return str(g.serialize(format='xml'))
    else:
        return json.loads(g.serialize(indent=4))


def getUniqueId():
    return socket.gethostname() + "-" + \
        str(os.getpid()) + "-" + str(uuid.uuid1())


def num(s):
    try:
        return int(s)
    except Exception:
        return float(s)



_d4p_plan_sqn = 0

class ProvenancePE(GenericPE):

    OUTPUT_METADATA = 'metadata'

    def pe_init(self, *args, **kwargs):
        # ProvenancePE.__init__(self)
        
        
        global _d4p_plan_sqn

        
        self.impcls = None
        
        if 'pe_class' in kwargs and kwargs['pe_class'] != GenericPE:
            self.impcls = kwargs['pe_class']

        if 'creator' not in kwargs:
            self.creator = None
        else:
            self.creator = kwargs['creator']

        self.error = ''

        if not hasattr(self, 'parameters'):
            self.parameters = {}
        if not hasattr(self, 'controlParameters'):
            self.controlParameters = {}

        out_md = {}
        out_md[NAME] = OUTPUT_METADATA

        # self.outputconnections[OUTPUT_DATA] = out1
        self.outputconnections[OUTPUT_METADATA] = out_md
        self.taskId = str(uuid.uuid1())

        # self.appParameters = None
        self.provon = True
        self.stateless = False
        self.derivationIds = list()
        self.iterationIndex = 0
        self.behalfOf=self.name+'_'+str(_d4p_plan_sqn)
        _d4p_plan_sqn=_d4p_plan_sqn+1
        
        
        
    def __init__(self, *args, **kwargs):
        GenericPE.__init__(self)
        self.parameters = {}
        

    def __getUniqueId(self):
        return socket.gethostname() + "-" + str(os.getpid()) + \
            "-" + str(uuid.uuid1())

    def getDataStreams(self, inputs):
        streams = {}
        for inp in self.inputconnections:
            if inp not in inputs:
                continue
            values = inputs[inp]
            if isinstance(values, list):
                data = values[0:]
            else:
                data = values
            streams["streams"].update({inp: data})
        return streams

    def getInputAt(self, port="input", index=0):
        return self.inputs[port][index]

    def _preprocess(self):
        self.instanceId = self.name + "-Instance-" + \
            "-" + getUniqueId()
        #self.log("CREATING INSTANCE: "+self.instanceId)
        super(ProvenancePE, self)._preprocess()
    
    
    def process(self, inputs):

        # streams = self.getDataStreams(inputs)
        self.stateless = False
        # processing...
        self.iterationIndex += 1
       #self.log(inputs)
       # self.log(total_size(inputs, 0))
       # self.insize=total_size(inputs, 0)
        self.__processwrapper(inputs)
        if not self.stateless:
            self.log('STATEFUL CAPTURE: ')
            if self.provon:
                self.extractProvenance(self, output_port=None)
                self.derivationIds = [self.derivationIds.pop()]

        if self.stateless:
            self.derivationIds = []
            self.stateless = False

    def extractItemMetadata(self, data, port='output'):
        
        return {}

    def flushData(self, data, metadata, port):
        trace = {}
        stream = data
        try:
            if self.provon:
                trace = self.packageAll(metadata)
                stream = self.prepareOutputStream(data, trace)

            try:
                super(ProvenancePE, self).write(port, stream)

            except:
                'if cant write doesnt matter move on'
                pass
            try:
                if self.provon:
                    # self.log('port: '+str(port))
                    super(
                        ProvenancePE,
                        self).write(
                        OUTPUT_METADATA,
                        trace['metadata'])
            except:
                self.log(traceback.format_exc())
                'if cant write doesnt matter move on'
                pass

            return True

        except Exception:
            self.log(traceback.format_exc())
            if self.provon:
                self.error += " FlushChunk Error: %s" % traceback.format_exc()

    def __processwrapper(self, data):
        try:

            self.initParameters()

            inputs = self.importInputData(data)
            # self.__importInputMetadata()
            return self.__computewrapper(inputs)

        except:
            self.log(traceback.format_exc())

    def initParameters(self):

        self.error = ''
        self.w3c_prov = {}
        self.stateless = False
        self.inMetaStreams = None
        self.username = None
        self.runId = None

        if self.provon:
            try:
                # self.iterationId = self.name + '-' + getUniqueId()
                if "username" in self.controlParameters:
                    self.username = self.controlParameters["username"]
                if "runId" in self.controlParameters:
                    self.runId = self.controlParameters["runId"]

            except:
                self.runId = ""
                pass

        self.outputdest = self.controlParameters[
            'outputdest'] if 'outputdest' in self.controlParameters else 'None'
        self.rootpath = self.controlParameters[
            'inputrootpath'] \
            if 'inputrootpath' in self.controlParameters else 'None'
        self.outputid = self.controlParameters[
            'outputid'] \
            if 'outputid' in self.controlParameters else 'None'

    def importInputData(self, data):
        
        inputs = {}
        
        try:
            if not isinstance(data, collections.Iterable):
                return data
            else:
                for x in data:
                    self.buildDerivation(data[x], port=x)
                    if '_d4p' in data[x]:
                        inputs[x] = data[x]['_d4p']
                    else:
                        inputs[x] = data[x]
                return inputs

        except Exception:
            self.output = ""
            self.error += "Reading Input Error: %s" % traceback.format_exc()
            raise

    def wirteResults(self, name, result):

        self.stateless = True

        if isinstance(result, dict) and '_d4p_prov' in result:
            meta = result['_d4p_prov']
            result = (result['_d4p_data'])

            if 'error' in meta:
                self.extractProvenance(result, output_port=name, **meta)
            else:

                self.extractProvenance(
                    result, error=self.error, output_port=name, **meta)

        else:
            self.extractProvenance(result, error=self.error, output_port=name)

    def __markIteration(self):
        self.startTime = datetime.datetime.utcnow()
        self.iterationId = self.name + '-' + getUniqueId()

    def __computewrapper(self, inputs):

        try:
            result = None

            self.__markIteration()

            if self.impcls is not None and isinstance(self, self.impcls):

                if hasattr(self, 'params'):
                    self.parameters = self.params

                result = self._process(inputs[self.impcls.INPUT_NAME])

                self.endTime = datetime.datetime.utcnow()

                if result is not None:
                    self.wirteResults(self.impcls.OUTPUT_NAME, result)
            else:
                result = self._process(inputs)
                # self.log=('REEEES :'+result)
                self.endTime = datetime.datetime.utcnow()

            if result is not None:
                return result

        except Exception:
            self.log(" Compute Error: %s" % traceback.format_exc())
            self.error += " Compute Error: %s" % traceback.format_exc()
            self.endTime = datetime.datetime.utcnow()
            self.wirteResults('error', {'error': 'null'})

    def prepareOutputStream(self, data, trace):
        ''' To be overridden '''

        try:

            # self.log("TRACE: "+str(trace))
            streamtransfer = {}

            streamtransfer['_d4p'] = data

            if self.provon:

                try:
                    streamtransfer['id'] = trace[
                        'metadata']["streams"][0]["id"]
                    streamtransfer[
                        "TriggeredByProcessIterationID"] = self.iterationId
                # self.log("WRITEOUT2: "+str(streamtransfer))
                    if not self.stateless:
                        # self.log(''' Building OUT Derivation ''')
                        self.buildDerivation(streamtransfer)
                except:
                    pass
            return streamtransfer

        except Exception:
            self.error += self.name + " Writing output Error: %s" % \
                traceback.format_exc()
            raise

    def packageAll(self, contentmeta):
        metadata = {}
        if self.provon:
            try:

                # self.log("content: "+str(contentmeta))
                # print "INSTANCE_ID: "+self.instanceId
                # print "UNIQUEEE: "+
                
                # identifies the actual iteration over the instance
                metadata.update({'iterationId': self.iterationId})
                # identifies the actual writing process'
                metadata.update({'actedOnBehalfOf': self.behalfOf})
                metadata.update({'_id': self.name+'_write_'+str(getUniqueId())})
                metadata.update({'iterationIndex': self.iterationIndex})
                metadata.update({'instanceId': self.instanceId})
                metadata.update({'annotations': {}})
                metadata.update({'stateful': not self.stateless})
                metadata.update({'worker': socket.gethostname()})
                metadata.update(
                    {'parameters': self.dicToKeyVal(self.parameters)})
                metadata.update({'errors': self.error})
                metadata.update({'pid': '%s' % (os.getpid())})
                metadata.update({'derivationIds': self.derivationIds})
                metadata.update({'name': self.name})
                metadata.update({'runId': self.runId})
                metadata.update({'username': self.username})
                metadata.update({'startTime': str(self.startTime)})
                metadata.update({'endTime': str(self.endTime)})
                metadata.update({'type': 'lineage'})
                metadata.update({'streams': contentmeta})
               # metadata.update({'input_size': self.insize})
                metadata.update({'mapping': sys.argv[1]})
                

                if self.creator is not None:
                    metadata.update({'creator': self.creator})
            except Exception:
                self.error += " Packaging Error: %s" % traceback.format_exc()

        output = {
            "metadata": metadata,
            "error": self.error,
            "pid": "%s" %
            (os.getpid(),
             )}
#        self.log("PACKAGED "+str(output))
        return output

    """
    Imports Input metadata if available, the metadata will be
    available in the self.inMetaStreams property as a Dictionary
    """

    def __importInputMetadata(self):
        try:
            self.inMetaStreams = self.input["metadata"]["streams"]
        except Exception:
            None

    """
    TBD: Produces a bulk output with data,location,format,metadata:
    to be used in exclusion of
    self.streamItemsLocations
    self.streamItemsFormat
    self.outputstreams
    """

    def extractProvenance(
            self,
            data,
            location="",
            format="",
            metadata={},
            control={},
            attributes={},
            error="",
            output_port="output"):

        self.error = error

        if isinstance(metadata, list):
            metadata.append(attributes)
        else:
            metadata.update(attributes)
        usermeta = {}

        if 'con:skip' in control and bool(control['con:skip']):
            self.provon = False
        else:
            self.provon = True
            usermeta = self.buildUserMetadata(
                data,
                location=location,
                format=format,
                metadata=metadata,
                control=control,
                attributes=attributes,
                error=error,
                output_port=output_port)

        self.flushData(data, usermeta, output_port)
        # self.flushData(data,{},output_port)

    """
    Overrides the GenericPE write
    """

    def write(self, name, data, **kwargs):

        #self.__markIteration()
        self.endTime = datetime.datetime.utcnow()
        if 'state_reset' in kwargs:
            self.stateless = bool(kwargs['state_reset'])
        else:
            self.stateless=True
        self.extractProvenance(data, output_port=name, **kwargs)

    """
    Reads and formats the stream's metadata
    """

    def __getMetadataWrapper(self, data):
        try:

            if self.provon:

                return {"streams": self.getMetadata(data)}

        except Exception:
                # streamlist=list()
                # streamItem={}
                # streammeta=list()
                # streamItem.update({"content": streammeta})
                # streamItem.update({"id":getUniqueId()});
                # streamlist.append(streamItem)
                # self.metadata.update({"streams":streamlist});
            traceback.print_exc(file=sys.stderr)
            self.error += self.name + " Metadata extraction Error: %s" % \
                traceback.format_exc()

    def getMetadata(self, data):
        streamlist = list()
        
        streamItem = {}
        streammeta = {}
        streammeta = {}
        self.extractItemMetadata(data)

        if not isinstance(streammeta, list):
            streammeta = self.streamItemsMeta[str(id(data))] if \
                isinstance(self.streamItemsMeta[str(id(data))], list) \
                else [self.streamItemsMeta[str(id(data))]]
        elif isinstance(streammeta, list):
            try:
                if isinstance(self.streamItemsMeta[str(id(data))], list):
                    streammeta = streammeta + \
                        self.streamItemsMeta[str(id(data))]
                if isinstance(self.streamItemsMeta[str(id(data))], dict):
                    streammeta.append(self.streamItemsMeta[str(id(data))])
            except:
                traceback.print_exc(file=sys.stderr)
                None

        streamItem.update({"content": streammeta})
        streamItem.update({"id": getUniqueId()})
        streamItem.update({"format": ""})
        streamItem.update({"location": ""})
        streamItem.update({"annotations": []})

        if (self.streamItemsPorts != {}):
            streamItem.update({"port": self.streamItemsPorts[str(id(data))]})
        if (self.streamItemsControl != {}):
            streamItem.update(self.streamItemsControl[str(id(data))])
        if (self.streamItemsLocations != {}):
            streamItem.update(
                {"location": self.streamItemsLocations[str(id(data))]})
        if (self.streamItemsFormat != {}):
            streamItem.update(
                {"format": self.streamItemsFormat[str(id(data))]})
        streamlist.append(streamItem)

        return streamlist

    def buildUserMetadata(self, data, **kwargs):
        streamlist = list()

        streamItem = {}
        streammeta = {}
        # streammeta={}
        streammeta = self.extractItemMetadata(data)
        # self.extractItemMetadata(data);
        # self.log(type(streammeta));
        if not isinstance(streammeta, list):
            streammeta = kwargs['metadata'] if isinstance(
                kwargs['metadata'], list) else [kwargs['metadata']]
        elif isinstance(streammeta, list):
            try:
                if isinstance(kwargs['metadata'], list):
                    streammeta = streammeta + kwargs['metadata']
                if isinstance(kwargs['metadata'], dict):
                    for y in streammeta:
                        y.update(kwargs['metadata'])
            except:
                traceback.print_exc(file=sys.stderr)
                None
        streamItem.update({"content": streammeta})
        streamItem.update({"id": getUniqueId()})
        streamItem.update({"format": ""})
        streamItem.update({"location": ""})
        streamItem.update({"annotations": []})
        # if (output_port in self.streamItemsPorts!={}):
        streamItem.update({"port": kwargs['output_port']})
        # if (self.streamItemsControl!={}):
        streamItem.update(kwargs['control'])
        # if (self.streamItemsLocations!={}):
        streamItem.update({"location": kwargs['location']})
        # if (self.streamItemsFormat!={}):
        streamItem.update({"format": kwargs['format']})
        streamItem.update({"size": total_size(data)})
        streamlist.append(streamItem)
        self.log(total_size(data, verbose=True))
        #self.log(data)
        return streamlist

    def buildDerivation(self, data, port=""):

        try:

            derivation = {'port': port, 'DerivedFromDatasetID':
                          data['id'], 'TriggeredByProcessIterationID':
                          data['TriggeredByProcessIterationID']}
            # self.log('Deriv: '+str(derivation))
            self.derivationIds.append(derivation)

        except Exception:
            if self.provon:
                None
                # traceback.print_exc(file=sys.stderr)
                # self.error+= " Build Derivation Error: %s" % \
                # traceback.format_exc()

    def dicToKeyVal(self, dict, valueToString=False):
        try:
            alist = list()
            for k, v in dict.iteritems():
                adic = {}
                adic.update({"key": str(k)})
                if valueToString:
                    adic.update({"val": str(v)})
                else:

                    try:
                        v = num(v)
                        adic.update({"val": v})
                    except Exception:
                        adic.update({"val": str(v)})

                alist.append(adic)

            return alist
        except Exception as err:

            self.error += self.name + " dicToKeyVal output Error: " + str(err)
            sys.stderr.write(
                'ERROR: ' +
                self.name +
                ' dicToKeyVal output Error: ' +
                str(err))
#                self.map.put("output","");
            traceback.print_exc(file=sys.stderr)


' This function dinamically extend the type of each the nodes of the graph '
' or subgraph with ProvenancePE type or its specialization'


def injectProv(object, provType, active=True, **kwargs):
    'Change grouping implementation '
    dispel4py.new.processor.GroupByCommunication.getDestination = \
        getDestination_prov

    if isinstance(object, WorkflowGraph):
        object.flatten()
        nodelist = object.getContainedObjects()
        for x in nodelist:
            injectProv(x, provType, **kwargs)
    else:
        print("Injecting provenance to: " + object.name +
              " Original type: " + str(object.__class__.__bases__))
        parent = object.__class__.__bases__[0]
        localname = object.name

        # if not isinstance(object,provType):
        #    provType.__init__(object,pe_class=parent, **kwargs)

        object.__class__ = type(str(object.__class__),
                                (provType, object.__class__), {})

        print("Injecting provenance to: " + object.name +
              " Transoformed: " + str(type(object)))
        object.pe_init(pe_class=parent, **kwargs)
        object.name = localname


' This methods enriches the graph to enable the production and recording '
' of run-specific provenance information the provRecorderClass parameter '
' can be used to attached several implementatin of ProvenanceRecorder '
' which could dump to files, dbs, external services, enrich '
' the metadata, etc..'


def InitiateNewRun(
        graph,
        provRecorderClass,
        provImpClass=ProvenancePE,
        input=[],
        username=None,
        workflowId=None,
        description="",
        system_id=None,
        workflowName=None,
        w3c_prov=False,
        runId=None):

    if username is None or workflowId is None or workflowName is None:
        raise Exception("Missing values")
    if runId is None:
        runId = getUniqueId()

    newrun = NewWorkflowRun()

    newrun.parameters = {"input": input,
                         "username": username,
                         "workflowId": workflowId,
                         "description": description,
                         "system_id": system_id,
                         "workflowName": workflowName,
                         "runId": runId,
                         "mapping": sys.argv[1]
                         }
    _graph = WorkflowGraph()
    provrec0 = provRecorderClass(toW3C=w3c_prov)
    _graph.connect(newrun, "output", provrec0, provrec0.INPUT_NAME)

    # attachProvenanceRecorderPE(_graph,provRecorderClass,runId,username,w3c_prov)

    # newrun.provon=True
    simple_process.process(_graph, {'NewWorkflowRun': [{'input': 'None'}]})

    injectProv(graph, provImpClass)
    attachProvenanceRecorderPE(
        graph,
        provRecorderClass,
        runId,
        username,
        w3c_prov)

    return runId


def attachProvenanceRecorderPE(
        graph,
        provRecorderClass,
        runId=None,
        username=None,
        w3c_prov=False):
    partitions = []
    try:
        partitions = graph.partitions
    except:
        print("NO PARTITIONS: " + str(partitions))

    if username is None or runId is None:
        raise Exception("Missing values")
    graph.flatten()

    nodelist = graph.getContainedObjects()

    provrecorder = provRecorderClass(toW3C=w3c_prov)
    recpartition = []
    for x in nodelist:
        if isinstance(x, (WorkflowGraph)):
            attachProvenanceRecorderPE(
                x,
                provRecorderClass,
                runId=runId,
                username=username,
                w3c_prov=w3c_prov)

        if isinstance(x, (ProvenancePE)) and x.provon:
            provrecorder = provRecorderClass(toW3C=w3c_prov)
            # provrecorder.numprocesses=1
            x.controlParameters["runId"] = runId
            x.controlParameters["username"] = username
            graph.connect(
                x,
                OUTPUT_METADATA,
                provrecorder,
                provrecorder.INPUT_NAME)
            recpartition.append(provrecorder)
            partitions.append([x])

    # partitions.append(recpartition)
    # graph.partitions=partitions
    return graph


class ProvenanceSimpleFunctionPE(ProvenancePE):

    def __init__(self, *args, **kwargs):

        self.__class__ = type(str(self.__class__),
                              (self.__class__, SimpleFunctionPE), {})
        SimpleFunctionPE.__init__(self, *args, **kwargs)
        # name=self.name
        ProvenancePE.__init__(self, self.name, *args, **kwargs)
        # self.name=type(self).__name__


class ProvenanceIterativePE(ProvenancePE):

    def __init__(self, *args, **kwargs):
        self.__class__ = type(str(self.__class__),
                              (self.__class__, IterativePE), {})
        IterativePE.__init__(self, *args, **kwargs)

        # name=self.name
        ProvenancePE.__init__(self, self.name, *args, **kwargs)


class NewWorkflowRun(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_output('output')

    def makeRunMetdataBundle(
            self,
            input=[],
            username=None,
            workflowId=None,
            description="",
            system_id=None,
            workflowName=None,
            w3c=False,
            runId=None):

        bundle = {}
        if username is None or workflowId is None or workflowName is None:
            raise Exception("Missing values")
        else:
            if runId is None:
                bundle["_id"] = getUniqueId()
            else:
                bundle["_id"] = runId

            bundle["runId"] = bundle["_id"]
            bundle["input"] = input
            bundle["startTime"] = str(datetime.datetime.utcnow())
            bundle["username"] = username
            bundle["workflowId"] = workflowId
            bundle["description"] = description
            bundle["system_id"] = system_id
            bundle["workflowName"] = workflowName
            bundle["mapping"] = self.parameters['mapping']
            bundle["type"] = "workflow_run"

        return bundle

    def _process(self, inputs):
        self.name = 'NewWorkflowRun'

        bundle = self.makeRunMetdataBundle(
            username=self.parameters["username"],
            input=self.parameters["input"],
            workflowId=self.parameters["workflowId"],
            description=self.parameters["description"],
            system_id=self.parameters["system_id"],
            workflowName=self.parameters["workflowName"],
            runId=self.parameters["runId"])
        print("RUN Metadata: " + str(bundle))

        self.write('output', bundle)


class ProvenanceRecorder(GenericPE):
    INPUT_NAME = 'metadata'

    def __init__(self, name='ProvenanceRecorder', toW3C=False):
        GenericPE.__init__(self)


class ProvenanceRecorderToFile(ProvenanceRecorder):

    INPUT_NAME = 'metadata'

    def __init__(self, name='ProvenanceRecorderToFile', toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.inputconnections[ProvenanceRecorder.INPUT_NAME] = {
            "name": ProvenanceRecorder.INPUT_NAME}

    def process(self, inputs):

        prov = inputs[self.INPUT_NAME]
        out = None

        if isinstance(prov, list) and "data" in prov[0]:

            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

        filep = open(os.environ['PROV_PATH'] + "/" + prov["_id"], "wr")
        json.dump(out, filep)


class ProvenanceRecorderToService(ProvenanceRecorder):

    REPOS_URL = ''

    def __init__(self, name='ProvenanceRecorderToService', toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.inputconnections[ProvenanceRecorder.INPUT_NAME] = {
            "name": ProvenanceRecorder.INPUT_NAME}

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorderToService.REPOS_URL)
        self.connection = httplib.HTTPConnection(
            self.provurl.netloc)

    def _process(self, inputs):
        prov = inputs[self.INPUT_NAME]
        out = None
        # print "PROVENANCETOSERIVCE:  "+str(prov)
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

        # self.log("TO SERVICE ________________ID: "+str(prov['_id']))
        params = urllib.urlencode({'prov': json.dumps(out)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"}
        self.connection.request(
            "POST",
            self.provurl.path,
            params,
            headers)

        response = self.connection.getresponse()
        print("Response From Provenance Serivce: ", response.status,
              response.reason, response, response.read())
        self.connection.close()
        return None

    def postprocess(self):
        self.connection.close()


class ProvenanceRecorderToServiceBulk(ProvenanceRecorder):

    REPOS_URL = ''

    def __init__(self, name='ProvenanceRecorderToServiceBulk', toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.bulk = []
        self.inputconnections[ProvenanceRecorder.INPUT_NAME] = {
            "name": ProvenanceRecorder.INPUT_NAME}
        self.timestamp = datetime.datetime.utcnow()

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorderToServiceBulk.REPOS_URL)
        
        self.connection = httplib.HTTPConnection(
            self.provurl.netloc)

    def postprocess(self):
        params = urllib.urlencode({'prov': json.dumps(self.bulk)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"}
        self.connection.request(
            "POST",
            self.provurl.path,
            params,
            headers)
        response = self.connection.getresponse()
        self.log("Postprocress: " +
                 str((response.status, response.reason, response,
                      response.read())))
        self.connection.close()
        self.bulk = []

    def _process(self, inputs):

        prov = inputs[self.INPUT_NAME]
        out = None
        # print "PROVENANCETOSERIVCE:  "+str(prov)
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

        self.bulk.append(out)

        if len(self.bulk) == 15:
            # self.log("TO SERVICE ________________ID: "+str(self.bulk))
            params = urllib.urlencode({'prov': json.dumps(self.bulk)})
            headers = {
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "application/json"}
            self.connection.request(
                "POST", self.provurl.path, params, headers)
            response = self.connection.getresponse()
            self.log("progress: " + str((response.status, response.reason,
                                         response, response.read())))
            self.connection.close()
            self.bulk = []

        return None


class ProvenanceRecorderToFileBulk(ProvenanceRecorder):

    INPUT_NAME = 'metadata'

    def __init__(self, name='ProvenanceRecorderToFileBulk', toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.inputconnections[ProvenanceRecorder.INPUT_NAME] = {
            "name": ProvenanceRecorder.INPUT_NAME}
        self.bulk = []

    def postprocess(self):
        filep = open(os.environ['PROV_PATH'] + "/bulk_" + getUniqueId(), "wr")
        json.dump(self.bulk, filep)
        self.bulk = []

    def process(self, inputs):

        prov = inputs[self.INPUT_NAME]
        out = None

        if isinstance(prov, list) and "data" in prov[0]:

            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

        self.bulk.append(out)
        if len(self.bulk) == 15:
            filep = open(
                os.environ['PROV_PATH'] +
                "/bulk_" +
                getUniqueId(),
                "wr")
            json.dump(self.bulk, filep)
