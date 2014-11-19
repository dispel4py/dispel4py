import types
from dispel4py.core import GenericPE
from processor import GenericWrapper, STATUS_ACTIVE, STATUS_TERMINATED
import processor


def simpleLogger(self, msg):
    print("%s: %s" % (self.id, msg))

def get_dependencies(proc, inputmappings):
    dep = []
    for input_name, sources in inputmappings[proc].iteritems():
        for s in sources:
            dep += get_dependencies(s, inputmappings)
            dep.append(s)
    return dep

def process(workflow, inputs={}):
    processes, inputmappings, outputmappings = processor.assign_and_connect(workflow, len(workflow.graph.nodes()))
    # print 'Processes: %s' % processes
    # print inputmappings
    # print outputmappings
    ordered = []
    for proc in outputmappings:
        if not outputmappings[proc]:
            dep = get_dependencies(proc, inputmappings)
            for n in ordered:
                try:
                    dep.remove(n)
                except:
                    # never mind if the element wasn't in the list
                    pass
            ordered += dep
            ordered.append(proc)
    
    proc_to_pe = {}
    for node in workflow.graph.nodes():
        pe = node.getContainedObject()
        proc_to_pe[processes[pe.id][0]] = pe
    
    simple = SimpleProcessingPE(ordered, inputmappings, outputmappings, proc_to_pe)
    wrapper = SimpleProcessingWrapper(simple,  [inputs])
    wrapper.targets = {}
    wrapper.sources = {}
    wrapper.process()
    
class SimpleProcessingWrapper(GenericWrapper):
    
    def __init__(self, pe, provided_inputs=None):
        GenericWrapper.__init__(self, pe)
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.provided_inputs = provided_inputs
        self.outputs = {}

    def _read(self):
        result = super(SimpleProcessingWrapper, self)._read()
        if result is not None:
            return result
        else:
            return None, STATUS_TERMINATED

    def _write(self, name, data):
        self.outputs[name] = data

class SimpleProcessingPE(GenericPE):
    def __init__(self, ordered, input_mappings, output_mappings, proc_to_pe):
        GenericPE.__init__(self)
        self.ordered = ordered
        self.input_mappings = input_mappings
        self.output_mappings = output_mappings
        self.proc_to_pe = proc_to_pe
    def _preprocess(self):
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.log = types.MethodType(simpleLogger, pe)
            pe.preprocess()
    def _postprocess(self):
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            pe.postprocess()
    def _process(self, inputs):
        all_inputs = {}
        results = {}
        for proc in self.ordered:
            pe = self.proc_to_pe[proc]
            output_mappings = self.output_mappings[proc]
            provided_inputs = processor.get_inputs(pe, inputs)
            try:
                provided_inputs.append(all_inputs[proc])
            except:
                try:
                    provided_inputs = all_inputs[proc]
                except:
                    pass
            for data in provided_inputs:
                pe.log('Processing input: %s' % data)
                result = pe.process(data)
                pe.log('Produced result: %s' % result)
                if result is not None:
                    for output_name in result:
                        try:
                            destinations = output_mappings[output_name]
                            for input_name, comm in destinations:
                                for p in comm.destinations:
                                    try:
                                        all_inputs[p].append({ input_name : result[output_name] })
                                    except:
                                        all_inputs[p] = [ { input_name : result[output_name] } ]
                        except KeyError:
                            # no destinations so this is a result of the PE
                            value = { output_name : result[output_name] }
                            try:
                                results[pe.id].append(value)
                            except KeyError:
                                results[pe.id] = [ value ]
        return results
                
class SimpleWriter(object):
    def __init__(self, pe, output, output_mappings):
        self.pe = pe
        self.output = output
        self.output_mappings = output_mappings
    def write(self, result):
        for output_name in result:
            destinations = self.output_mappings[output_name]
            for input_name, comm in destinations:
                for p in comm.destinations:
                    self.output[p] = result[output_name]
     
