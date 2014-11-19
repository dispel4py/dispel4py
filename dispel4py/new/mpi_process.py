from mpi4py import MPI
        
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

from processor import GenericWrapper, simpleLogger, STATUS_TERMINATED, STATUS_ACTIVE    
import processor
import types

def process(workflow, inputs={}):
    processes={}
    inputmappings = {}
    outputmappings = {}
    success=True
    if rank == 0:
        try:
            processes, inputmappings, outputmappings = processor.assign_and_connect(workflow, size)
            print 'Processes: %s' % processes
        except:
            print 'Not enough processes for execution of graph'
            success=False
    success=comm.bcast(success,root=0)
    if not success:
        return
        
    processes=comm.bcast(processes,root=0)
    inputmappings=comm.bcast(inputmappings,root=0)
    outputmappings=comm.bcast(outputmappings,root=0)
    
    for node in workflow.graph.nodes():
        pe = node.getContainedObject()
        if rank in processes[pe.id]:
            provided_inputs = processor.get_inputs(pe, inputs)
            wrapper = MPIWrapper(pe, provided_inputs)
            wrapper.targets = outputmappings[rank]
            wrapper.sources = inputmappings[rank]
            wrapper.process()

class MPIWrapper(GenericWrapper):
    
    def __init__(self, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.rank = rank
        self.provided_inputs = provided_inputs
        self.terminated = 0

    def _read(self):
        result = super(MPIWrapper, self)._read()
        if result is not None:
            return result
            
        status = MPI.Status()
        msg=comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        while tag == STATUS_TERMINATED:
            self.terminated += 1
            if self.terminated >= self._num_sources:
                break
            else:
               msg=comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
               tag = status.Get_tag()
        return msg, tag

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
                self.pe.log('Sending %s to %s' % (output, i))
                request=comm.isend(output, tag=STATUS_ACTIVE, dest=i)
                status = MPI.Status()
                request.Wait(status)
                
    def _terminate(self):
        for output, targets in self.targets.iteritems():
            for (inputName, communication) in targets:
                for i in communication.destinations:
                    self.pe.log('Terminating consumer %s' % i)
                    request=comm.isend(None, tag=STATUS_TERMINATED, dest=i)
