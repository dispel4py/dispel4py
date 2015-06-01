Concepts
========

Basic processing elements
-------------------------

PEs form nodes in dispel4py data-flow graphs. Each PE captures a particular data-handling or data-processing step. There may be many instances of the same PE in a dispel4py graph.
A library of standard PEs is provided, there are many libraries of specialised PEs for particular application fields, and users can define their own PEs, as described here.
A PE normally takes data units from its inputs and deals with them one at a time, emitting data units on its outputs. However, some PEs may need to collect a set of data units before they can process them and generate the resultant output.

PEs in dispel4py are Python classes and extend the class :py:class:`dispel4py.core.GenericPE`.
This class has several methods that must or may be overridden.
Please refer to the documentation of the class :py:class:`dispel4py.core.GenericPE` for further information.

PEs have named inputs and outputs which can be connected to other PEs.
A PE *must* declare the input and output connections that it provides as this is required information 
when the graph is translated into the enactment process.
For example, to declare a graph with one input ``in1`` and one output ``out1``::

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input('in1')
        self._add_output('out1')

A PE *may* implement custom processing by overriding the :py:func:`~dispel4py.core.GenericPE._process` method. 
This method is called for each data unit in an input stream. If the PE has no inputs the method is called at least once, and the number of iterations is controlled by the mapping when enacting the graph (see the chapter :doc:`/enactment` for more details).

The inputs parameter is a dictionary which maps the name of an input connection to the corresponding input data unit.
Note that at least one input will have input data when :py:func:`~dispel4py.core.GenericPE._process` is called (unless the PE has no inputs), but some inputs may be empty.

The (optional) return value is a dictionary mapping names of output connections to the corresponding output data units. If the PE produces no data in an iteration it must return ``None``. If the PE produces up to one data item per output in an iteration, it may be returned as a dictionary. If a PE produces one or more data units in an iteration these may be written to the target output streams at any time during :py:func:`~dispel4py.core.GenericPE._process` by calling :py:func:`~dispel4py.core.GenericPE.write`.


The example shows show how to produce output after applying ``myfunc`` to the input::

    def _process(self, inputs):
        data = inputs['in1']
        result = myfunc(data)
        return { 'out1' : result }

Alternatively the data item may be written to the output streams like this::

    def _process(self, inputs):
        data = inputs['in1']
        result = myfunc(data)
        self.write('out1', result)

The method :py:func:`~dispel4py.core.GenericPE.log` can be used for log statements when implementing custom PEs. 
The enactment engine takes care of providing a logging mechanism for a particular environment.
For example, a standalone enactment process would print the log messages to stdout.

Initialisation of variables before the start of the processing loop can be implemented by overriding :py:func:`~dispel4py.core.GenericPE.preprocess`.

If the user does not choose to perform local development runs then it is possible to submit a PE from a client that does not provide all of the libraries used in the process() function. Since a client doesn't execute :py:func:`~dispel4py.core.GenericPE._process` it is possible to use the PE in the definition of a dispel4py workflow by ensuring that any corresponding :py:exc:`ImportError` is caught and ignored. For example, the PE below uses an ObsPy (http://obspy.org) module when processing but doesn't require it to be available on the client for the graph definition so we catch the error and only print a warning message::

    try:
        from obspy.core import Stream
    except ImportError:
        print "Warning: Could not import 'obspy.core'"
        pass


PE base classes
---------------

Base classes for various patterns are available that may be extended or modified when implementing custom PEs:

* :py:class:`dispel4py.base.BasePE` - a PE that is initialised with a list of input and output names.
* :py:class:`dispel4py.base.IterativePE` - a PE that declares one input named  ``input`` and one output named ``output``. Subclasses implement the method :py:func:`~dispel4py.base.IterativePE._process`.
* :py:class:`dispel4py.base.ConsumerPE` - a PE that has one input named ``input`` and no outputs. Subclasses implement the method :py:func:`~dispel4py.base.ConsumerPE._process`.
* :py:class:`dispel4py.base.ProducerPE` - a PE that has no inputs and one output named ``output``. Subclasses implement the method :py:func:`~dispel4py.base.ProducerPE._process`.
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
