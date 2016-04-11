
# coding: utf-8

# In[1]:

from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *
import time
import random
import pickle
from dispel4py.base import create_iterative_chain, GenericPE, ConsumerPE, IterativePE, SimpleFunctionPE
import marshal, types


class Source(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        #self._add_input('input2')
    
    def _process(self,inputs):
        print inputs
        iter=None
        if 'input' in inputs:
            self.log('from input')
            iter=inputs['input'][0]
        if 'input2' in inputs:
            self.log('from input2')
            iter=inputs['input2'][0]
            
         
        #self.addToProv(0,metadata={'this':"mine"})
        
        while (iter>=5):
            time.sleep(0.002)
            self.write('output',iter,metadata={'iter':iter})
            iter=iter-5
        
        
        


def square(data,prov_cluster):
    data=data*data
    prov={'format':'Random float', 'metadata':{'value_s':data}}
    #print ("SQUARE: "+str(data))
    return {'_d4p_prov':prov,'_d4p_data':data} 
    #return data


class Div(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('input')
        self._add_output('output')
        self.prov_cluster='cluster'
        self.operands=[]
        #self.par='A'
        
        
        
    def new_process(self,data):
        self.log("I AM NEWWWWW")
        self.operands.append(data['input'])
        if (len(self.operands)==2):
            val = (self.operands[0]-1)/self.operands[1]
            self.write('output',val,metadata={'new_val':val})
            self.log("New Imp!!!! "+str(val))
            self.operands=[]
        
        
    def _process(self,data):
        #self.log("SSSSSS: "+str(self.par))
        self.operands.append(data['input'])
        #self.addToProv(0,metadata={'that':"yours"})
        
        val=0
        if (len(self.operands)==2):
            #time.sleep(0.5)
            val = self.operands[0]/self.operands[1]
            self.write('output',val,metadata={'val':val})
            self.log(val)
            self.operands=[]
            
    'must be implemented to handle provenance feedbacks'
#   
    def _process_feedback(self,data):
        code = pickle.loads(data)
        self.log("SOME Feedback procedure implemented: "+str(code))
        Div._process = code

         

    
    


sc = Source()
sc.name='PE_source'


squaref=SimpleFunctionPE(square,{'prov_cluster':'cluster'})
divf=Div()
divf.name='PE_div'


#processes=[squaref,divf]
#chain = create_iterative_chain(processes, FunctionPE_class=SimpleFunctionPE)

#Initialise the graph
graph = WorkflowGraph()

#Common way of composing the graph
graph.connect(sc,'output',squaref,'input')
graph.connect(squaref,'output',divf,'input')
#graph.connect(divf,'output',sc,'input2')
#graph.connect(divf,'output',squaref,'input')
 

# Alternatively with pipeline array
#Create pipelines from functions

#graph.connect(sc,'output',chain,'input')


#graph.flatten()

#Prepare Input
input_data = {"PE_source": [{"input": [25]},{"input": [45]}]}

#Launch in simple process
#simple_process.process_and_return(graph, input_data)





# In[2]:

ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8082/workflow/insert'
rid='RDWD_'+getUniqueId()
InitiateNewRun(graph,ProvenanceRecorderToServiceBulk,provImpClass=ProvenancePE,username='aspinuso',runId=rid,w3c_prov=False,workflowName="test_rdwd",workflowId="xx",clustersRecorders={'cluster':ProvenanceRecorderToServiceWFeedback},feedbackPEs=['PE_div'])


#from IPython.display import HTML
#HTML("<iframe src='http://127.0.01:8080/provenance-explorer/html/d3js.jsp?level=PE&runId="+rid+"' width=800 height=800></iframe>")


# In[3]:




# In[4]:

#simple_process.process_and_return(graph, input_data)


# In[ ]:




# In[ ]:



