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

'''
The core module for dispel4py.
'''

import uuid
# Connection-level dict elements
NAME = 'name'
TYPE = 'type'
DESCRIPTION = 'descr'
META = 'meta'
GROUPING = 'grouping'
WRITER = 'writer'

class GenericPE(object):
    ''' 
Base class for Dispel4Py processing elements (PEs). Custom PEs are expected to extend this class and 
override the 'process' function.

Custom PEs must override :py:func:`~dispel4py.core.GenericPE.__init__` to declare the inputs and outputs that can be connected within
the workflow graph, by defining a NAME and possibly a TYPE.
The type of a connection is specific to the enactment system. In the example below the target system 
is Storm and the type declares what kind of tuples are produced::
    out1[TYPE] = ['timestamp', 'origin', 'streams']
    
In some cases, the output types are determined dynamically and depend on the input types, 
for example when implementing a filter which consumes any type of blocks but the type of the output 
is the same as the type of the input.
The graph framework supports this by propagating types across the workflow before enactment and 
providing each PE with the input types that it can expect in the method::
    setInputTypes(self, types)
which can be overridden to deduce output types from input types or to raise an error if the types are 
not acceptable.
The PE may then override the method::
    getOutputTypes(self)
to declare the output types that it produces. 
In the example of a filter PE this method would return the input types.

Custom PEs may implement the method preprocess() to initialise variables or data before processing 
commences.
   
Example implementation::
 
    import traceback
    import cStringIO
    import base64
    from obspy.core import read,UTCDateTime,Stream,Trace
    from dispel4py.core import GenericPE, NAME, TYPE

    INPUT_NAME = 'input'
    OUTPUT_NAME = 'output'

    class AppendAndSynchronize(GenericPE):

        def __init__(self):
            GenericPE.__init__(self)
            self._add_input(INPUT_NAME)
            self._add_output(OUTPUT_NAME, ['timestamp', 'origin', 'streams'])

        def process(self, inputs):
            values = inputs[INPUT_NAME]
            parameters = values[0]
            origin = values[1]
            data = values[2:]
            if not data:
                self.error+= "No Data";
                raise Exception("No Data!")

            streams=list();
            while data:
                streamItem=data.pop(0);
                streams.append(eval(streamItem["data"]))

            # Reads the first file
            st = read(self.rootpath+"%s" % (streams[0].pop(0),))
            
            #Reads the following files
            while streams[0]:
                ff= "%s" % (streams[0].pop(0),)
                st += read(self.rootpath+ff) 

            starttime="%s" % (parameters["starttime"])
            endtime="%s" % (parameters["endtime"])
            
            st=st.slice(UTCDateTime(starttime),UTCDateTime(endtime));
            streamtransfer={}
            if type(st) == Stream:
                memory_file = cStringIO.StringIO()
                mseed = st.write(memory_file, format="MSEED")
                streamtransfer={"data":base64.b64encode(memory_file.getvalue())}
            output = [ parameters, origin, streamtransfer ]

            return { OUTPUT_NAME : output }
    '''
    
    def __init__(self,numprocesses=1):
        self.inputconnections = {}
        self.outputconnections = {}
        self.wrapper = 'simple'
        self.pickleIgnore = []
        self.pickleIgnore = vars(self).keys()
        self.numprocesses = numprocesses

        def log(self, message):
            ''' To be implemented '''
            pass
        self.log = log
        self.name = self.__class__.__name__
        self.id = self.name + str(uuid.uuid4())
        
    def _add_input(self, name, grouping=None, tuple_type=None):
        '''
        Declares an input for this PE. 
        This method may be used when initialising a PE instead of modifying 
        :py:attr:`~dispel4py.core.GenericPE.inputconnections` directly.
        
        :param name: name of the input
        :param grouping: the grouping type that this input expects (optional)
        :param tuple_type: type of tuples accepted by this input (optional)
        '''
        self.inputconnections[name] = { NAME : name }
        if grouping:
            self.inputconnections[name][GROUPING] = grouping
        if tuple_type:
            self.inputconnections[name][TYPE] = tuple_type
    
    def _add_output(self, name, tuple_type=None):
        '''
        Declares an output for this PE. 
        This method may be used when initialising a PE instead of modifying 
        :py:attr:`~dispel4py.core.GenericPE.outputconnections` directly.
        
        :param name: name of the output
        :param tuple_type: type of tuples produced by this output (optional)
        '''
        self.outputconnections[name] = { NAME : name }
        if tuple_type:
            self.outputconnections[name][TYPE] = tuple_type
    
    def setInputTypes(self, types):
        ''' 
        Sets the input types of this PE, in the form of a dictionary. It is meant to be overridden, 
        e.g. if output types depend on input. 
        
        .. note::
        
            This method is always called before :py:func:`~dispel4py.core.GenericPE.getOutputTypes`.

        :param types: object types for each input stream
        :type types: dictionary mapping input name to input type
                
        Usage example::
        
            pe.setInputTypes({'input1':['t1', 't2', 't3'], 'input2':['t4', 't5']})
        '''
        pass
    
    def getOutputTypes(self):
        ''' 
        Returns the output types of this PE, in the form of a dictionary. This method may be overridden if 
        output types are not static and depend on input types.
        
        .. note::
            
            This method is only called after the input types have been initialised in :py:func:`~dispel4py.core.GenericPE.setInputTypes`. 
        
        :rtype: a dictionary mapping each output name to its type
        
        By default it returns a dictionary of the types defined in the 'outputconnections' instance variable.
        
        Usage example::
        
            def getOutputTypes(self):
                output = { 'output1' : myInputs['input1'], 'output2' : [ 'comment' ] }
        
        '''
        ret = {}
        for name, output in self.outputconnections.iteritems():
            try:
                ret[name] = output[TYPE]
            except KeyError:
                raise Exception("%s: No output type defined for '%s'" % (self.id, name))
        return ret
        
    def preprocess(self):
        '''
        It is called once before processing commences, e.g. for variable and data initialisation.
        '''
        None
        
    def process(self, inputs):
	    ''' 
        (To be overridden by a PE implementation subclass.)   
        The 'inputs' dictionary contains data from any or all of the streams that are connected to
        this PE, in any order. The return value of this function is a single output dictionary, with 
        the names of the output streams as keys. To produce more than one output data can be written 
        at any point during processing using the :py:func:`~dispel4py.core.GenericPE.write` method.
        
        :param inputs: the input data for this iteration
        :type inputs: dictionary
        :rtype: a dictionary with the output data     
        '''
	    None
    
    def postprocess(self):
        '''
        This method is called once after the last block has been processed and a terminate message was sent to this PE.
        '''
        None
        
    def write(self, name, data):
        '''
        This writes the 'data' to the output pipe with name 'name' of this PE.
        '''
        try:
            output = self.outputconnections[name]
        except KeyError:
            raise Exception("Can't write data: Unknown output connection '%s' for PE '%s'" % (name, type(self).__name__))
        output[WRITER].write(data)
            
        
class LockstepPE(GenericPE):
    '''
    Representation of a PE which consumes its input in lockstep. The inputs dictionary that is passed to 
    the :py:func:`~dispel4py.GenericPE.LockStepPE.process` function is guaranteed to contain one data item 
    from each of the connected input streams.
    '''
    
    def __init__(self):
        GenericPE.__init__(self)
        self.wrapper = 'lockstep'
            
class SourcePE(GenericPE):
    '''
    Representation of data-producing PE, i.e. a PE with no input connections.
    '''
    
    def __init__(self):
        GenericPE.__init__(self)
        self.wrapper = 'source'
