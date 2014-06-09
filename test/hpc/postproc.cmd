#!/bin/bash
# DO NOT USE environment = COPY_ALL
#@ job_type=MPICH
#@ class=micro
#@ node=1
#@ total_tasks=16
#@ wall_clock_limit=1:00:00
#@ job_name = postproc
#@ network.MPI = sn_all,not_shared,us
#@ initialdir = $(home)/dispy
#@ output = job$(jobid).out
#@ error = job$(jobid).err
#@ notification=always
#@ notify_user=a.krause@epcc.ed.ac.uk
#@ queue
. /etc/profile
. /etc/profile.d/modules.sh
#setup of environment
module load python/2.7.5 specfem3d
module unload mpi.ibm
module load mpi.intel
module load mpi4py
export PYTHONPATH=$PYTHONPATH:/home/hpc/pr45lo/di72zaz/python/lib/python2.7/site-packages/
cd dispy
mpiexec -n 16 python -m verce.worker_mpi test.hpc.postproc_graph graph 1
