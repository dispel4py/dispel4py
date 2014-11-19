import copy
import multiprocessing
import types
from processor import GenericWrapper, STATUS_ACTIVE, STATUS_TERMINATED, simpleLogger
import processor

def _processWorker(wrapper):
    wrapper.process()

def process(workflow, size, inputs={}):
    try:
        processes, inputmappings, outputmappings = processor.assign_and_connect(workflow, size)
        print 'Processes: %s' % processes
    except:
        print 'Not enough processes for execution of graph'
        return
    process_pes = {}
    queues = {}
    for node in workflow.graph.nodes():
        pe = node.getContainedObject()
        provided_inputs = processor.get_inputs(pe, inputs)
        for proc in processes[pe.id]:
            cp = copy.deepcopy(pe)
            cp.rank = proc
            cp.log = types.MethodType(simpleLogger, cp)
            wrapper = MultiProcessingWrapper(proc, cp, provided_inputs)
            process_pes[proc] = wrapper
            wrapper.input_queue = multiprocessing.Queue()
            wrapper.input_queue.name = 'Queue_%s_%s' % (cp.id, cp.rank)
            queues[proc] = wrapper.input_queue
            wrapper.targets = outputmappings[proc]
            wrapper.sources = inputmappings[proc]
    for proc in process_pes:
        wrapper = process_pes[proc]
        wrapper.output_queues = {}
        for target in wrapper.targets.values():
            for inp, comm in target:
                for i in comm.destinations:
                    wrapper.output_queues[i] = queues[i]

    jobs = []
    for wrapper in process_pes.values():
        p = multiprocessing.Process(target=_processWorker, args=(wrapper,))
        jobs.append(p)

    for j in jobs:
        j.start()
        
    for j in jobs:
        j.join()
    

class MultiProcessingWrapper(GenericWrapper):
    
    def __init__(self, rank, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.provided_inputs = provided_inputs
        self.terminated = 0

    def _read(self):
        result = super(MultiProcessingWrapper, self)._read()
        if result is not None:
            return result
        # read from input queue
        data, status = self.input_queue.get()
        print 'read %s' % str((data, status))
        while status == STATUS_TERMINATED:
            self.terminated += 1
            if self.terminated >= self._num_sources:
                return data, status
            else:
                data, status = self.input_queue.get()
        return data, status

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