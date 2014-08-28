.. .. documentation master file, created by
   sphinx-quickstart on Mon Aug 11 11:22:11 2014.

Dispel4Py API documentation
===========================

.. rubric:: Core modules

.. autosummary::
   :nosignatures:
   :toctree: .
   
   dispel4py.core
   dispel4py.base
   dispel4py.utils
   dispel4py.visualisation
   dispel4py.workflow_graph
   
.. rubric:: Enactment platforms

.. autosummary::
   :nosignatures:
   :toctree: .

   dispel4py.simple_process
   dispel4py.multi_process
   dispel4py.partition
   dispel4py.worker_mpi
   dispel4py.storm.storm_submission
   dispel4py.storm.topology


.. rubric:: Examples and tests

.. autosummary::
    :nosignatures:
    :toctree: .
    
    dispel4py.examples.graph_testing.pipeline_test
    dispel4py.examples.graph_testing.split_merge
    dispel4py.examples.graph_testing.teecopy
    dispel4py.examples.graph_testing.group_by
    dispel4py.examples.graph_testing.grouping_alltoone
    dispel4py.examples.graph_testing.grouping_onetoall
    dispel4py.examples.graph_testing.grouping_split_merge
    dispel4py.examples.graph_testing.parallel_pipeline
    dispel4py.examples.graph_testing.partition_parallel_pipeline
    dispel4py.examples.graph_testing.pipeline_composite
    dispel4py.examples.graph_testing.word_count
    dispel4py.examples.graph_testing.word_count_filter
    dispel4py.examples.graph_testing.testing_PEs
    dispel4py.test.simple_process_test
    dispel4py.test.multi_process_test
    dispel4py.test.worker_mpi_test
    

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

