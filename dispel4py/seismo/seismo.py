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

from dispel4py.provenance import ProvenancePE, OUTPUT_DATA, INPUT_NAME
from dispel4py.core import TYPE
import uuid
try:
    from obspy.core.utcdatetime import UTCDateTime
except ImportError:
    pass


class SeismoPE(ProvenancePE):

    INPUT_NAME = INPUT_NAME
    OUTPUT_DATA = OUTPUT_DATA

    def __init__(self):
        ProvenancePE.__init__(self)
        self.outputconnections[OUTPUT_DATA][TYPE] = \
            ['timestamp', 'location', 'streams']

    def getDataStreams(self, inputs):
        values = inputs[INPUT_NAME]
        self._timestamp = values[0]
        self._location = values[1]
        data = values[2:]
        streams = {"streams": data}
        return streams

    def writeOutputStreams(self, outputs):
        output_data = [self._timestamp, self._location]
        for v in outputs["streams"]:
            output_data.append(v)
        result = {OUTPUT_DATA: output_data}
        return result

    def initParameters(self, streams):
        ProvenancePE.initParameters(self, streams)
        self.parameters.update(self._timestamp)
        self.parameters.update(self._location)

    def extractItemMetadata(self, st):
        try:
            streammeta = list()
            for tr in st:
                metadic = {}
                metadic.update({"id": str(uuid.uuid1())})
                for attr, value in tr.stats.__dict__.iteritems():
                    if attr == "mseed":
                        mseed = {}
                        for a, v in value.__dict__.iteritems():
                            try:
                                if type(v) == UTCDateTime:
                                    mseed.update({a: str(v)})
                                else:
                                    mseed.update({a: float(v)})
                            except Exception:
                                mseed.update({a: str(v)})
                        metadic.update({"mseed": mseed})
                    else:
                        try:
                            if type(value) == UTCDateTime:
                                metadic.update({attr: str(value)})
                            else:
                                metadic.update({attr: float(value)})
                        except Exception:
                            metadic.update({attr: str(value)})
                streammeta.append(metadic)
        except Exception:
            streammeta = str(st)
        return streammeta

    def writeStream(self, st, attr):
        streamtransfer = {'data': st, 'attr': attr}
        output_data = [self._timestamp, self._location, streamtransfer]
        self.write(OUTPUT_DATA, output_data)
