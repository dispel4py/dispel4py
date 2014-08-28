.. Dispel4Py documentation master file, created by
   sphinx-quickstart on Mon Mar 24 11:35:17 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Dispel4Py's documentation!
=====================================

Dispel4Py is a Python library for describing **abstract workflows** for **distributed data-intensive applications**. 

* **Abstract:** users don't need to worry about the properties of underlying middleware, implementations or systems.
* **Workflow:** workflows represent an alternative way to program in a modular, reusable and exchangeable fashion.
* **Distributed:** dispel4py is designed for programming in large, heterogeneous, distributed systems. Abstract workflows get translated and *enacted*-executed in a number of contexts, such as Apache Storm and MPI-powered clusters.
* **Data-intensive:** as *data-intensive* we describe the applications which are complex due to data-volume or algorithmic reasons. dispel4py employs the streaming model for dealing with large volumes of data over distributed systems, or with complex data-driven algorithms.

Dispel4Py provides executable **mappings** to a number of enactment systems.

* **MPI**: Systems that implement the Message Passing Interface 
* **Storm**: a free and open source distributed realtime computation system. 
* **sequential**: local mapping for testing during the development process.
* **multiprocessing**: a Python implementation leveraging multiple processors on shared memory systems.

Contents
--------

.. toctree::
   :maxdepth: 2
   
   intro
   processing_elements
   registry
   enactment
   tools
   api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

