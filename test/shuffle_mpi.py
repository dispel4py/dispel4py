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

# Needs at least 5 processes

# Run this with 
#mpiexec -n 5 python shuffle_mpi.py

from mpi4py import MPI

import numpy

from numpy import random

from verce.GenericPE import GenericPE

comm=MPI.COMM_WORLD

rank=comm.Get_rank()

size=comm.Get_size()

NAME='name'

TYPE='type'


class ProcessToNode():
	def __init__(self,rank):
		self.rank=rank
		self.rank_dest=None
		self.rank_source=None

class Node():
	def __init__(self,distribution=None,communication=None):
		self.processes=[]
		self.pe=None
		self.spout=False
		self.bolt=False
		self.num_proc=0
		self.node_input=None
		self.node_output=None
		if distribution is None:
			self.distribution="shuffle"
		else:
			self.distribution=distribution
		if communication is None:
			self.communication="round_robin"
		else:
			self.communication=communication

def processWrapper(rank,pe,input=None):
	output=pe.process(input)
    	print 'Rank {0}: this is my input {1}, this is my process {2}\n'.format(rank,input,output)
	return output

def distributionWrapper(rank,data,rank_dest,distribution):
	if distribution == "shuffle" :
    		print 'Rank {0}:sending output is {1}, to the process {2}\n'.format(rank,data,rank_dest)
		comm.isend(data, dest=rank_dest)

def receiveWrapper(process,processes_list):
	if process.rank_source is None:
		process.rank_source=processes_list[0]
		
	else:
		next=processes_list.index(process.rank_source)+1
		if next >= len(processes_list):
			next=0
		process.rank_source=processes_list[next]
	input=comm.recv(source=process.rank_source)
    	print 'Rank {0}:receiving input is {1}, from the process {2}\n'.format(rank,input,process.rank_source)
	return input

def communicationWrapper(process,processes_list, communication):
	if communication == "round_robin":	
		if process.rank_dest is None:
			process.rank_dest=processes_list[0]
	
		else:
			next=processes_list.index(process.rank_dest)+1
			if next >= len(processes_list):
				next=0
			process.rank_dest=processes_list[next]

	if communication == "all_to_one":
		process.rank_dest=processes_list[0]

	

class RandomFilter(GenericPE):

    input_name = 'input'

    output_name = 'output'

    def __init__(self):

        GenericPE.__init__(self)

        self.inputconnections['input'] = { NAME : 'input' }

        out1 = {}

        out1[NAME] = "output"

        self.outputconnections["output"] = out1

    

    def process(self, inputs):

        #if random.choice([True, False]):
	return { 'output' : inputs['output'] }

            

class WordCounter(GenericPE):

    input_name = 'input'

    output_name = 'output'

    def __init__(self):

        GenericPE.__init__(self)

        self.inputconnections['input'] = { NAME : 'input' }

        out1 = {}

        out1[NAME] = "output"

        self.outputconnections["output"] = out1

        self.mywords = {}

    

    def process(self, inputs):

        word = inputs['output']

        try:

            self.mywords[word] += 1

        except KeyError:

            self.mywords[word] = 1

        return { 'output' : [word, self.mywords[word]]}

class RandomWordProducer(GenericPE):

    words = ["VERCE", "Earthquake", "Computing", "Seismology", "Modelling", "Analysis", "Infrastructure"]

    def __init__(self):

        GenericPE.__init__(self)

        out1 = {}

        out1[NAME] = "output"

        self.outputconnections["output"] = out1

    def process(self, inputs=None):

        outputs = {}

        outputs["output"] = random.choice(RandomWordProducer.words)

        return outputs

        




num_nodes=3
graph=[]
for i in range(num_nodes):
	graph.append(Node())

process=ProcessToNode(rank)

graph[0].spout=True
graph[1].bolt=True
graph[2].bolt=True

graph[0].num_proc=1
graph[1].num_proc=2
graph[2].num_proc=2

graph[0].processes.append(0)

graph[1].processes.append(1)
graph[1].processes.append(2)

graph[2].processes.append(3)
graph[2].processes.append(4)

graph[0].pe=RandomWordProducer()
graph[1].pe=RandomFilter()
graph[2].pe=WordCounter()

graph[0].node_output=1
graph[1].node_input=0
graph[1].node_output=2
graph[2].node_input=1
	
iter=4

for g in graph:
	num_iter=iter/g.num_proc
	print "num_iter is", num_iter
	for i in range(num_iter):
		if process.rank in g.processes:
			print"----------------rank:",rank,"iter",i
			if g.spout is True :
				output=processWrapper(process.rank,g.pe,input=None)
				communicationWrapper(process,graph[g.node_output].processes,g.communication)
				distributionWrapper(process.rank,output,process.rank_dest,g.distribution)
			else:
				input=receiveWrapper(process,graph[g.node_input].processes)
				output=processWrapper(process.rank,g.pe,input)
				if g.node_output is not None:
					communicationWrapper(process,graph[g.node_output].processes,g.communication)
					distributionWrapper(process.rank,output,process.rank_dest,g.distribution)
		else:
			pass
	
print "end of computation"
