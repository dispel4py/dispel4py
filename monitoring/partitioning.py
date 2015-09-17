from collections import defaultdict


class Node(object):

    def __init__(self, pe, pt):
        self.pes = [pe]
        self.comm_out = defaultdict(float)
        self.comm_in = defaultdict(float)
        self.processing_time = pt

    def add(self, node):
        self.pes += node.pes
        self.processing_time += node.processing_time
        for source_node in node.comm_in:
            source_node.comm_out[self] = source_node.comm_out[node]
            del source_node.comm_out[node]
            self.comm_in[source_node] = node.comm_in[source_node]
        for dest_node in node.comm_out:
            self.comm_out[dest_node] = node.comm_out[dest_node]


def find_best_partitioning(job, pe_times, comm_times):
    graph = start_graph(job, pe_times, comm_times)
    roots = find_roots(comm_times)
    nodes = [graph[pe] for pe in roots]
    partitions = []
    while nodes:
        source_node = nodes.pop()
        partitions.append(source_node)
        expand(source_node, nodes, partitions)
    return [p.pes for p in partitions]


def expand(source_node, not_visited, partitions):
    node_list = list(source_node.comm_out.keys())
    while node_list:
        # print([n.pes for n in node_list])
        dest_node = node_list.pop(0)
        print('%s > min(%s, %s)' % (source_node.comm_out[dest_node],
                                    source_node.processing_time,
                                    dest_node.processing_time))
        if source_node.comm_out[dest_node] > \
                min(source_node.processing_time,
                    dest_node.processing_time):
            # same partition
            # print('adding %s to node %s' % (dest_node.pes, source_node.pes))
            if dest_node in partitions:
                partitions.remove(dest_node)
            node_list += dest_node.comm_out.keys()
            source_node.add(dest_node)
        else:
            # print('new partition for %s' % dest_node.pes)
            not_visited.append(dest_node)
            partitions.append(dest_node)


def find_roots(comm_times):
    roots = set()
    has_inputs = set()
    for key, value in comm_times.items():
        source_pe = key[0]
        # output = key[1]
        dest_pe = key[2]
        # input = key[3]
        roots.add(source_pe)
        has_inputs.add(dest_pe)
    roots.difference_update(has_inputs)
    return roots


def start_graph(job, pe_times, comm_times):
    processing_times = {}
    for t in pe_times:
        processing_times[t['_id']['pe']] = t['time']
    nodes = {}
    for pe, pt in processing_times.items():
        node = Node(pe, pt)
        nodes[pe] = node
    for key, value in comm_times.items():
        source_pe = key[0]
        # output = key[1]
        dest_pe = key[2]
        # input = key[3]
        nodes[dest_pe].comm_in[nodes[source_pe]] += value['time']
        nodes[source_pe].comm_out[nodes[dest_pe]] += value['time']
    return nodes
