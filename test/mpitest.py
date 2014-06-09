# Copyright (c) The University of Edinburgh 2014
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#	 Unless required by applicable law or agreed to in writing, software
#	 distributed under the License is distributed on an "AS IS" BASIS,
#	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#	 See the License for the specific language governing permissions and
#	 limitations under the License.

# Needs at least 4 processes
# Run this with 
# mpiexec -n 4 python -m test.mpitest

from test.testing import RandomWordProducer, RandomFilter, WordCounter

from mpi4py import MPI

comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()

def sourceWrapper(outputDest, producer, numIterations=None):
    x = 0
    while numIterations is None or x < numIterations:
        x+=1
        result = producer.process()
        if result:
            print 'Send %s to %s' % (result, outputDest)
            comm.send(result, dest=outputDest)

def shuffleDistribute(inputSource, outputDest, workers):
    dataQueue = []
    while True:
        status = MPI.Status()
        result = comm.recv(status=status, source=MPI.ANY_SOURCE)
        msgSource = status.source
        # print 'Received data from %s' % msgSource
        if msgSource == inputSource:
            # we have some input
            dataQueue.append(result)
        else:
            # send result to the next queue
            if result:
                print 'sending result to next PE %s' %result
                # comm.send(result, dest=outputDest)
            workers.append(msgSource)
        if dataQueue and workers:
            sink = workers.pop()
            # print 'Sending data to %s' % sink
            comm.send(dataQueue.pop(), dest=sink)
            
def groupByDistribute(inputSource, outputDest, workers, groupBy):
    availableWorkers = list(workers)
    numWorkers = len(availableWorkers)
    dataQueue = dict([x, []] for x in availableWorkers)
    while True:
        status = MPI.Status()
        result = comm.recv(status=status, source=MPI.ANY_SOURCE)
        msgSource = status.source
        print 'Received data from %s: %s' % (msgSource, result)
        if msgSource == inputSource:
            # we have some input
            group = [ result['output'][x] for x in groupBy ]
            index = hash(tuple(group)) % numWorkers
            # print 'index of data %s' %index
            dataQueue[workers[index]].append(result)
        else:
            # send result to the next queue
            if result:
                print 'sending result to next PE %s' %result
                # comm.send(result, dest=outputDest)
            availableWorkers.append(msgSource)
        if dataQueue and availableWorkers:
            for sink in dataQueue:
                try:
                    availableWorkers.remove(sink)
                    # print 'Sending data to %s' % sink
                    if dataQueue[sink]:
                        outp = dataQueue[sink].pop()
                        # translate output to input
                        inp = { 'input' : outp['output'] }
                        comm.send(inp, dest=sink)
                except ValueError:
                    pass
                    
def processWrapper(pe, master):
   while True:
       data=comm.recv(source=master)
       # print 'received data %s from master' %data
       k=pe.process(data)
       comm.send(k, dest=master)
       print 'my rank is %s and my input is %s and output is %s' % (rank, data, k)
    
# The following assumes there are 4 processes:
# rank 0 is the source, rank 1 is master that distributes to slaves 2 and 3

if rank == 0:
    words = RandomWordProducer()
    sourceWrapper(1, words, numIterations=100)
elif rank == 1:
    # shuffleDistribute(0, None, [2, 3])
    groupByDistribute(0, None, [2, 3], [0])
else:
    # pe=RandomFilter()
    pe = WordCounter()
    processWrapper(pe,1)
