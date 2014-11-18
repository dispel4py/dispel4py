import sys
import traceback
import types

STATUS_ACTIVE = 1
STATUS_INACTIVE = 2
STATUS_TERMINATED = 3

def simpleLogger(self, msg):
    print("%s (rank %s): %s" % (self.id, self.rank, msg))

class GenericWrapper(object):

    def __init__(self, pe):
        self.pe = pe
        self.targets = {}

    def process(self):
        self.pe.preprocess()
        result = self._read()
        self.pe.log('Read result: ' + str(result))
        inputs, status = result
        while status != STATUS_TERMINATED:
            if inputs is not None:
                outputs = self.pe.process(inputs)
                # print 'Produced output: %s'% outputs
                if outputs is not None:
                    for key, value in outputs.iteritems():
                        self._write(key, value)
            inputs, status = self._read()
        self.pe.postprocess()
        self._terminate()
        
    def _read(self):
        # check the provided inputs
        if self.provided_inputs is not None:
            if self.provided_inputs:
                return self.provided_inputs.pop(0), STATUS_ACTIVE
            else:
                return None, STATUS_TERMINATED
        
class ShuffleCommunication(object):
    def __init__(self, rank, sources, destinations):
        self.destinations=destinations
        self.currentIndex = (sources.index(rank) % len(self.destinations)) -1
    def getDestination(self, data):
        self.currentIndex = (self.currentIndex+1)%len(self.destinations)
        return [self.destinations[self.currentIndex]]

class GroupByCommunication(object):
    def __init__(self, destinations, input_name, groupby):
        self.groupby = groupby
        self.destinations=destinations
        self.input_name = input_name
    def getDestination(self,data):
        output = tuple([data[self.input_name][x] for x in self.groupby])
        dest_index=abs(make_hash(output))%len(self.destinations)
        return [self.destinations[dest_index]]

class AllToOneCommunication(object):
    def __init__(self, destinations):
        self.destinations=destinations
    def getDestination(self,data):
        return [self.destinations[0]]
        
class OneToAllCommunication(object):
    def __init__(self, destinations):
        self.destinations=destinations
    def getDestination(self,data):
        return self.destinations

def assign_processes(num_processes, graph)




class MultiProcessingWrapper(GenericWrapper):
    
    def __init__(self, rank, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.provided_inputs = provided_inputs

    def _read(self):
        result = super(MultiProcessingWrapper, self)._read()
        if result is not None:
            return result
        # read from input queue
        return self.input_queue.get()

    def _write(self, name, data):
        try:
            targets = self.targets[name]
        except KeyError:
            # no targets
            return
        for (inputName, communication) in targets:
            output = { inputName : data }
            dest = communication.getDestination(output)
            for i in dest:
                self.pe.log('Writing out %s' % output)
                self.output_queues[i].put((output, STATUS_ACTIVE))
                
    def _terminate(self):
        for output, targets in self.targets.iteritems():
            for (inputName, communication) in targets:
                for i in communication.destinations:
                    self.pe.log('Terminating consumer %s' % i)
                    self.output_queues[i].put((None, STATUS_TERMINATED))