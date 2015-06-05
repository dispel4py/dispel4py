Introduction
============

This tutorial is an introduction to dispel4py. We will see how to write dispel4py PEs, how to connect them together to form a workflow and how this workflow is executed in different environments.

How to write a PE
=================

In this section we are going to implement our first PE.

First you need to decide what kind of processing the PE will do and what the data units are that it processes. In our example we are implementing a PE that decides if a number is divisible by another number. The PE is configured with this divisor and for each input data item it tests whether the number can be divided by this divisor. It sends the input data item to its output if it is not divisible.


Create a PE class
-----------------

To start with we create a PE that does only very few things::

    from dispel4py.base import IterativePE

    class MyFirstPE(IterativePE):

        def __init__(self, divisor):
            IterativePE.__init__(self)
            self.divisor = divisor

In this case we extend the base class :py:class:`dispel4py.base.IterativePE` which defines one input and one output, which is exactly what we need. We pass the divisor as an initialisation parameter to the object which stores it.


Implement the processing method
-------------------------------

Now the actual work begins: We have to implement the processing method. This is done by overriding the method of the superclass::

        def _process(self, data):
            ...

We fill in the processing commands, in our case this means that we test if the input data item is divisible by our divisor, and return it if it is not divisible::

        def _process(self, data):
            if not data % self.divisor:
                return data

The data returned by ``_process`` is written to the output stream of the PE.

That's it! Our first PE is complete::

    from dispel4py.base import IterativePE

    class MyFirstPE(IterativePE):

        def __init__(self, divisor):
            IterativePE.__init__(self)
            self.divisor = divisor

        def _process(self, data):
            if not data % self.divisor:
                return data

Create a simple workflow
========================

In this section we are going to create a workflow, using the PE that we implemented in the previous section. There's a useful PE in the library of dispel4py PEs that just produces a sequence of numbers. 

We can connect this number producer to our PE which is initialised with the divisor 3 in this example::

    from dispel4py.workflow_graph import WorkflowGraph
    from dispel4py.examples.graph_testing.testing_PEs import TestProducer
    
    producer = TestProducer()
    divide = MyFirstPE(3)
    
    graph = WorkflowGraph()
    graph.connect(producer, 'output', divide, 'input')

This workflow produces integers and tests whether they are divisible by 3. Any numbers that are not divisible by 3 will be written to the output. 

Now save the whole file as ``myfirstgraph.py``.


Execute the workflow
====================

To run this workflow you can use the sequential simple processor::

    $ dispel4py simple myfirstgraph.py

This produces the following output::

    Processing 1 iteration.
    Inputs: {'TestProducer2': 1}
    SimplePE: Processed 1 iteration.
    Outputs: {'MyFirstPE3': {'output': [1]}}

By default, without providing any input, the producer PE only processes once and only produces one number, the number 1 which is not divisible by 3 so this is the output stream of our workflow.

In the output above, you can see that PEs are assigned an integer ID to uniquely identify them within the graph, as you can use more than one PE of the same kind in a graph. In this graph the producer PE is assigned the ID ``TestProducer2`` which is a combination of its class name and a number, and ``MyFirstPE3`` is the ID of our own PE.

To run more than one iteration, you can specify the number with the parameter ``-i``, say 20 times::

    $ dispel4py simple myfirstgraph.py -i 20
    Processing 20 iterations.
    Inputs: {'TestProducer2': 20}
    SimplePE: Processed 1 iteration.
    Outputs: {'MyFirstPE3': {'output': [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19, 20]}}

The output of this workflow are the numbers in the range from 1 to 20 which are not divisible by 3.


Write a data producer PE
========================

Producing the input
-------------------

Next we will create a ProducerPE that creates the input for our sieve of Eratosthenes. The test producer that we were using above only produces one number per iteration. In our case we would like to create a PE that produces all the integers from a start value up to (and excluding) an upper bound.

The implementation looks like this::

    from dispel4py.base import ProducerPE

    class IntegerProducer(ProducerPE):
        def __init__(self, start, limit):
            ProducerPE.__init__(self)
            self.start = start
            self.limit = limit
        def _process(self):
            for i in xrange(self.start, self.limit):
                self.write(ProducerPE.OUTPUT_NAME, i)

This introduces several new concepts. The ProducerPE is a base class which has no inputs and one output named ``output``. We initialise the IntegerProducer PE with the lower and upper bounds of the range that we want to produce.

In the implementation of the ``_process`` method we iterate over the range of integers from the lower bound up to the upper bound. Since the processing method generates more than one data item we have to write them one at a time to the output data stream using the :py:func:`~dispel4py.core.GenericPE.write` method.


Using the producer in the workflow
----------------------------------

Now we hook our own producer into the workflow, replacing the TestProducer from the dispel4py library::

    from dispel4py.workflow_graph import WorkflowGraph

    producer = IntegerProducer(2, 100)
    divide = MyFirstPE(3)

    graph = WorkflowGraph()
    graph.connect(producer, 'output', divide, 'input')

Everything else stays the same. We create an instance of the IntegerProducer that outputs the range of numbers from 2 to 99 (excluding the upper bound of 100).

Now execute the new workflow using the simple mapping::

    $ dispel4py simple myfirstgraph.py
    Processing 1 iteration.
    Inputs: {'IntegerProducer2': 1}
    SimplePE: Processed 1 iteration.
    Outputs: {'MyFirstPE3': {'output': [2, 4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 19, 20, 22, 23, 25, 26, 28, 29, 31, 32, 34, 35, 37, 38, 40, 41, 43, 44, 46, 47, 49, 50, 52, 53, 55, 56, 58, 59, 61, 62, 64, 65, 67, 68, 70, 71, 73, 74, 76, 77, 79, 80, 82, 83, 85, 86, 88, 89, 91, 92, 94, 95, 97, 98]}}

The data generated by ``MyFirstPE`` is the list of integers in the range from 2 to 99 that are not divisible by 3.

Custom PE names
===============

You can assign a custom name to your PE instead of using the class name to aid readability of the output logs, in particular if there are several PEs of the same type in the graph. For example::

    divide2 = MyFirstPE(2)
    divide2.name = 'Div(2)_'
    divide3 = MyFirstPE(3)
    divide3.name = 'Div(3)_'

and the output would look like this::

    Outputs: {'Div(2)_4': {'output': [3, 5, 7, 9 ... 97, 99]}, 'Div(3)_5': {'output': [2, 4, 5, 7, ... 95, 97, 98]}}

Parallel processing
===================

For this very simple case we can easily parallelise the execution of the workflow. To do this we use the dispel4py multi mapping that executes a workflow in multiple processes using the Python multiprocessing [#]_ library::

    $ dispel4py multi myfirstgraph.py -n 4
    Processing 1 iteration.
    Processes: {''IntegerProducer2': [0], 'MyFirstPE3': [1, 2, 3]}
    Range(2,99)_2 (rank 0): Processed 1 iteration.
    MyFirstPE3 (rank 3): Processed 32 iterations.
    MyFirstPE3 (rank 1): Processed 33 iterations.
    MyFirstPE3 (rank 2): Processed 33 iterations.


.. note:: No changes or additional instructions were necessary in order to execute the graph in a parallel environment.

This example executes the workflow using 4 processes as indicated by the command line parameter "``-n 4``". The output shows which PE is assigned to which processes::

    Processes: {'IntegerProducer2': [0], 'MyFirstPE3': [1, 2, 3]}

In this case, ``MyFirstPE`` is assigned to processes 1, 2 and 3, so there are three parallel instances, and ``IntegerProducer`` is assigned to process 0. (Root PEs always have only one instance to avoid generating duplicate data.) The instances of ``MyFirstPE`` each process about a third of the data, as you can see from their output when processing is complete::

    MyFirstPE3 (rank 1): Processed 33 iterations.
    MyFirstPE3 (rank 2): Processed 33 iterations.
    MyFirstPE3 (rank 3): Processed 32 iterations.


.. note:: Note that when executing in a parallel environment the output from each PE is not collected as in the simple mapping. You are responsible for collecting this output and printing or storing it.


-----

References
==========

.. [#] https://docs.python.org/2/library/multiprocessing.html

