Introduction
============

Dispel4Py is a Python library used to describe abstract workflows for distributed data-intensive applications. 
These workflows are compositions of processing elements representing knowledge discovery activities (such as batch database querying, noise filtering and data aggregation) through which significant volumes of data can be streamed in order to manufacture a useful knowledge artefact. 
Such processing elements may themselves be defined by compositions of other, more fundamental computational elements, in essence having their own internal workflows. 
Users can construct workflows importing existing processing elements from a registry, or can define their own, recording them in a registry for later use by themselves or others.

Abstract dataflows described in Dispel4Py can be executed in numerous environments, for example using a Storm cluster.
Thus Dispel4Py allows to construct workflows without particular knowledge of the specific context in which they are to be executed, granting them greater generic applicability.

Let's start with a short example::

	from dispel4py.workflow_graph import WorkflowGraph

	# Connect to a remote registry
	from dispel4py import registry
	reg = registry.initRegistry()

	# Import existing processing elements from the registry
	from dispel4py.test.RandomWordProducer import RandomWordProducer
	from dispel4py.test.Filter import RandomFilter

	# Create the components of the workflow graph
	words = RandomWordProducer()
	filter = RandomFilter()
	
	# Connect PEs together to form a graph
	graph = WorkflowGraph()
	graph.connect(words, 'output', filter, 'input')

This example illustrates a number of important features of Dispel4Py:
 * Importing shared processing elements from a remote registry
 * Using and configuring processing elements
 * Creating an abstract workflow graph and indicating the data flow by connecting processing elements

We will introduce these concepts in more depth in the following chapters.


**Workflows**
	A workflow is a description of a distributed data-intensive application based on a streaming-data execution model. 
	It specifies the computational processes needed and the data dependencies that exist between those processes. 
	Each data element in the stream of inputs is processed by specialised computational elements which then pass on data to the next element; data is transferred using an interprocess communication network.

**Processing Elements**
	A processing element (PE) is a computational activity which encapsulates an algorithm, services and other data transformation processes — as such, PEs represent the basic computational blocks of any Dispel4Py workflow. 
 	Users can import existing processing elements from a remote registry, or register new PEs, either by writing new ones or by creating compositions of existing PEs.
	A Dispel4Py processing element has a specific structure: a class that extends :py:class:`dispel4py.GenericPE.GenericPE` and overrides the :py:func:`~dispel4py.GenericPE.GenericPE.process` method. In addition, a PE must indicate its specification as a graph node: 
	 * The names of inputs and outputs
	 * The input types that are consumed by the PE
	 * The output types that are produced by the PE
