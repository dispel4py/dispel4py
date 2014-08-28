Concepts
========

Basic processing elements
-------------------------

Processing elements in Dispel4Py extend the class :py:class:`dispel4py.core.GenericPE`.
This class has several methods that must or may be overridden.
Please refer to the API documentation of the class for further information.

PEs have named inputs and outputs which can be connected to other PEs.
A PE *must* declare the input and output connections that it provides as this is required information 
when the graph is translated into the enactment process.
For example, to declare a graph with one input ``in1`` and one output ``out1``::

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('in1')
        self._add_output('out1')

A PE *may* implement custom processing by overriding the :py:func:`~dispel4py.GenericPE.GenericPE.process` method. 
This method is called for each data block in an input stream.
The inputs parameter is a dictionary which maps the name of an input connection to the corresponding input data block.
Note that at least one input will have input data when :py:func:`~dispel4py.GenericPE.GenericPE.process` is called, but some inputs may be empty.
The output data must be a dictionary mapping names of output connections to the corresponding output data blocks.

The example shows show how to produce output after applying ``myfunc`` to the input::

    def process(self, inputs):
        data = inputs['in1']
        result = myfunc(data)
        return { 'out1' : result }

If a PE produces more than one output block in an iteration these can be written to output streams at any time during :py:func:`~dispel4py.core.GenericPE.process` by calling :py:func:`~dispel4py.core.GenericPE.write`.

The method :py:func:`~dispel4py.core.GenericPE.log` can be used for log statements when implementing custom PEs. 
The enactment engine takes care of providing a logging mechanism for a particular environment.
For example, a standalone enactment process would print the log messages to stdout.

Initialisation of variables before the start of the processing loop can be implemented by overriding :py:func:`~dispel4py.core.GenericPE.preprocess`.

Note that is possible to submit a PE from a client that does not support all of the libraries used in the processing function. Since a client doesn't execute :py:func:`~dispel4py.core.GenericPE.process` it is possible to use the PE in the definition of a Dispel4Py workflow by ensuring that any corresponding :py:exc:`ImportError` is caught and ignored. For example, in the following the PE uses an obspy module when processing but doesn't require it to be available on the client for the graph definition::

    try:
        from obspy.core import Stream
    except ImportError:
        print "Warning: Could not import 'obspy.core'"
        pass

A PE can be registered in a remote registry, identified by a package name (the module) and its class name.

PE base classes
---------------

Base classes for various patterns are available that may be extended or modified when implementing custom PEs:

* :py:class:`dispel4py.base.BasePE` - a PE that is initialised with a list of input and output names.
* :py:class:`dispel4py.base.IterativePE` - a PE that declares one input and one output. Subclasses implement the method :py:func:`~dispel4py.base.IterativePE._process`.
* :py:class:`dispel4py.base.ConsumerPE` - a PE that has one input and no outputs. Subclasses implement the method :py:func:`~dispel4py.base.ConsumerPE._process`.
* :py:class:`dispel4py.base.SimpleFunctionPE` - This PE calls a function with the input data for each processing iteration. The function is specified when instantiating this PE.


Composite processing elements
-----------------------------

Composite processing elements are PEs that contain subgraphs.

To create a composite PE first create a workflow graph, for example::

	wordfilter = WorkflowGraph()
	words = RandomWordProducer()
	filter = RandomFilter()
	wordfilter.connect(words, 'output', filter, 'input')
	
Now define the inputs and outputs of this subgraph by mapping a name to a pair ``(PE, name)`` that identifies an input or output within the subgraph::
	
	wordfilter.inputmappings = { }
	wordfilter.outputmappings = { 'out' : (filter, 'output') }
	
The above statements define that the composite PE containing the subgraph has no inputs and one output named ``output`` which is the output of the PE ``filter``.
	
Now the subworkflow can be used in another workflow and connected to a PE::

	normalise = AnotherFilter()
	toplevel = WorkflowGraph()
	toplevel.connect(wordfilter, 'out', normalise, 'input')
	
    
Functions
---------

Functions are Python methods that can be registered in a remote registry. 
Usually functions create and configure PEs or subgraphs.

The helper method :py:func:`dispel4py.base.create_iterative_chain` is a function that creates a pipeline of :py:class:`~dispel4py.base.SimpleFunctionPE` objects and returns this pipeline as a *composite processing element*. The following example shows how to create a pipeline of simple mathematical operations (``addTwo``, ``divideByTwo``, etc) that is then applied to the numbers produced by an instance of :py:class:`dispel4py.examples.graph_testing.testing_PEs.TestProducer`::

    from dispel4py.base import create_iterative_chain
    from dispel4py.examples.graph_testing.testing_PEs import TestProducer
    from dispel4py.workflow_graph import WorkflowGraph

    def addTwo(data):
        return 2 + data
    
    def multiplyByFour(data):
        return 4 * data

    def divideByTwo(data):
        return data/2

    def subtract(data, n):
        return data - n
    
    functions = [ addTwo, multiplyByFour, divideByTwo, (subtract, { 'n' : 1 }) ]
    composite = create_iterative_chain(functions)
    producer = TestProducer()

    graph = WorkflowGraph()
    graph.connect(producer, 'output', composite, 'input')
