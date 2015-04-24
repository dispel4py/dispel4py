import argparse
import types


def simpleLogger(self, msg):
    print("%s: %s" % (self.id, msg))


class PEWrapper(object):

    def __init__(self, pe):
        self.pe = pe
        self.pe.log = types.MethodType(simpleLogger, pe)
        self.pe.preprocess()

    def process(self, data):
        self.pe.log('processing %s' % data)
        for output, desc in self.pe.outputconnections.items():
            desc['writer'] = SimpleWriter(output)
        result = self.pe.process(data)
        self.pe.log('result: %s' % result)
        written = []
        if result is not None:
            written.append(result)
        for output, desc in self.pe.outputconnections.items():
            written.extend(desc['writer'].data)
        self.pe.log('writing: %s' % written)
        return written


class SimpleWriter(object):

    def __init__(self, output_name):
        self.output_name = output_name
        self.data = []

    def write(self, data):
        self.data.append({'output': data})


def parse_args(args, namespace):
    parser = argparse.ArgumentParser(
        description='Submit a dispel4py graph to Apache Spark.')
    parser.add_argument('-m', '--master', help='master URL for the cluster')
    parser.add_argument('-n', '--name', help='name of the Spark process')
    parser.add_argument(
        '-d',
        '--deploy-mode',
        choices=['cluster', 'client'],
        help='deploy driver on worker nodes or locally as external client')
    result = parser.parse_args(args, namespace)
    return result


class Projection(object):

    def __init__(self, outputs):
        self.outputs = outputs

    def project(self, data):
        result = {}
        for o in self.outputs:
            if o in data:
                result[o] = data[o]
        if result:
            return [result]
        else:
            return []


class Rename(object):

    def __init__(self, mapping):
        self.mapping = mapping

    def rename(self, data):
        print 'renaming data: %s, mapping is: %s' % (data, self.mapping,)
        result = {}
        for o, i in self.mapping.items():
            if o in data:
                result[i] = data[o]
        if result:
            return [result]
        else:
            return []


def process(sc, workflow, inputs, args):

    from dispel4py.new.processor\
        import assign_and_connect, _order_by_dependency
    graph = workflow.graph
    result = assign_and_connect(workflow, graph.number_of_nodes())
    if result is None:
        return

    processes, inputmappings, outputmappings = result
    process_to_pes = {}
    wrappers = {}
    for node in workflow.graph.nodes():
        pe = node.getContainedObject()
        wrapper = PEWrapper(pe)
        for p in processes[pe.id]:
            process_to_pes[p] = pe
            wrappers[p] = wrapper
    print 'Processes: %s' % processes
    print inputmappings
    print outputmappings
    ordered = _order_by_dependency(inputmappings, outputmappings)
    print 'Ordered processes: %s' % ordered
    output_rdd = {}
    result_rdd = {}

    for proc in ordered:
        inps = inputmappings[proc]
        outs = outputmappings[proc]
        wrapper = wrappers[proc]
        if inps:
            if len(inps) == 1:
                for input_name, sources in inps.iteritems():
                    inp_rdd = output_rdd[(sources[0], input_name)]
                out_rdd = inp_rdd.flatMap(wrapper.process)
            else:
                prev = None
                for input_name, sources in inps.iteritems():
                    inp_rdd = output_rdd[(sources[0], input_name)]
                    if prev:
                        inp_rdd = prev.union(inp_rdd)
                    prev = inp_rdd
                out_rdd = prev.flatMap(wrapper.process)
            if len(outs) == 1:
                for output_name in outs:
                    for inp in outs[output_name]:
                        input_name = inp[0]
                        rename = Rename({output_name: input_name})
                        output_rdd[(proc, input_name)] = \
                            out_rdd.flatMap(rename.rename)
            else:
                for output_name in outs:
                    proj = Projection([output_name])
                    proj_rdd = out_rdd.flatMap(proj.project)
                    for inp in outs[output_name]:
                        rename = Rename({output_name: inp[0]})
                        output_rdd[(proc, inp[0])] = \
                            proj_rdd.flatMap(rename.rename)
            if not outs:
                result_rdd[proc] = out_rdd

        else:
            # need to think about how we're providing static inputs
            pe_input = inputs
            out_rdd = sc.textFile(pe_input, 1).flatMap(wrapper.process)
            if len(outs) == 1:
                for output_name in outs:
                    print 'connecting %s' % output_name
                    for inp in outs[output_name]:
                        input_name = inp[0]
                        rename = Rename({output_name: input_name})
                        output_rdd[(proc, input_name)] = \
                            out_rdd.flatMap(rename.rename)
            else:
                for output_name in outs:
                    proj = Projection([output_name])
                    out_rdd = out_rdd.flatMap(proj.project).persist()
                    for inp in outs[output_name]:
                        input_name = inp[0]
                        rename = Rename({output_name: input_name})
                        output_rdd[(proc, input_name)] = \
                            out_rdd.flatMap(rename.rename)
            if not outs:
                result_rdd[proc] = out_rdd
    print "RESULT PROCESSES: %s" % result_rdd.keys()
    for p in result_rdd:
        result = result_rdd[p].collect()
        print 'RESULT FROM %s: %s' % (p, result)
        for x in result:
            print x


def main():
    from pyspark import SparkContext, SparkConf

    conf = SparkConf()
    conf.setAppName('dispel4py')
    conf.set("spark.storage.memoryFraction", "0.5")
    sc = SparkContext(
        conf=conf,
        pyFiles=['dispel4py/dist/dispel4py-1.0.1-py2.7.egg'])

    from dispel4py.new import processor
    from dispel4py.utils import load_graph

    parser = processor.create_arg_parser()
    args, remaining = parser.parse_known_args()
    args = parse_args(remaining, args)

    graph = load_graph(args.module, args.attr)
    process(sc, graph, inputs=args.data, args=args)

if __name__ == '__main__':
    main()
