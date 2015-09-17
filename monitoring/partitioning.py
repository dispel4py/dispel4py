from collections import defaultdict


class Node(object):

    def __init__(self, pe, pt):
        self.pes = [pe]
        # maps a destination node to the communication times with that node
        self.comm_out = defaultdict(float)
        # maps a source node to the communication times with that node
        self.comm_in = defaultdict(float)
        self.processing_time = pt
        self.is_root = False

    def add(self, node):
        self.pes += node.pes
        self.processing_time += node.processing_time
        for source_node in node.comm_in:
            source_node.comm_out[self] = source_node.comm_out[node]
            del source_node.comm_out[node]
            self.comm_in[source_node] = node.comm_in[source_node]
        for dest_node in node.comm_out:
            self.comm_out[dest_node] = node.comm_out[dest_node]


def assign_processes(partitions, total_processes):
    total_processing_time = 0
    num_roots = 0
    for node in partitions:
        if node.is_root:
            node.num_processes = 1
            num_roots += 1
        else:
            total_processing_time += node.processing_time
            node.num_processes = 0
    # remove the root processes that have been assigned already
    n_processes = total_processes - num_roots
    total_assigned = 0
    for node in partitions:
        if not node.is_root:
            frac = node.processing_time / total_processing_time
            np = round(frac * n_processes)
            node.num_processes += np
            total_assigned += np
    misfit = total_assigned - n_processes
    while misfit:
        for node in partitions:
            if misfit > 0:
                if not node.is_root and node.num_processes > 1:
                    node.num_processes -= 1
                    misfit -= 1
            elif misfit < 0:
                if not node.is_root:
                    node.num_processes += 1
                    misfit += 1
            else:
                break
    return [int(node.num_processes) for node in partitions]


def find_best_partitioning(job, pe_times, comm_times):
    graph = start_graph(job, pe_times, comm_times)
    roots = find_roots(comm_times)
    nodes = []
    for pe in roots:
        graph[pe].is_root = True
        nodes += graph[pe].comm_out.keys()
    partitions = [graph[pe] for pe in roots]
    while nodes:
        source_node = nodes.pop()
        if source_node not in partitions:
            partitions.append(source_node)
        expand(source_node, nodes, partitions)
    return partitions


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
            if dest_node not in partitions:
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
