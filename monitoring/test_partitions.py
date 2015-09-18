import sys
from monitoring_server import collect_diagnostics
from partitioning import find_best_partitioning, assign_processes, draw_dot
job = "a1ce23a0-54a5-417d-8cd2-d76984f9be89"
job = sys.argv[1]
# job = 'd77bb249-c9ea-4797-b852-87875c5c8970'
# find_best_partitioning(job)
print('Collecting diagnostics for job %s' % job)
pe_times, comm_times = collect_diagnostics(job)
print('Computing partitions:')
partitions = find_best_partitioning(job, pe_times, comm_times)
print([p.pes for p in partitions])
procs = assign_processes(partitions, int(sys.argv[2]))
print(procs)
print(draw_dot(partitions, comm_times))
