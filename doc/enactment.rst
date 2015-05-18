Enactment of dispel4py workflows
================================

A workflow must be submitted for execution over available resources in order to produce results. 
dispel4py provides various mappings for a number of enactment engines.


Sequential Processing
---------------------

This mapping serves as an enactment engine for processing small datasets, typically during development or when testing a new installation.
The processing elements of the graph are executed in sequence by a single process.

To execute a dispel4py graph by using the simple (sequential) mapping run the following::

    $ dispel4py simple <module> \
                [-a graph attribute within the module] \
                [-f file containing the input dataset in JSON format] \
                [-d input data in JSON format] \
                [-i number of iterations]

The ``module`` parameter is the the file path or the name of the python module that creates the workflow graph.

The parameter ``-a`` is the name of the graph attribute in the module. If there is only one dispel4py workflow graph in the module it is optional as the graph object is detected automatically. If the module creates more than one graph, for example if using composite PEs, the object name of the graph must be provided.

If an input file is specified using ``-f`` then the parameters ``-i`` and ``-d`` are ignored.
When using ``-d`` then the parameter ``-i`` is ignored.
The number of iterations provided with ``-i`` applies to all of the root PEs of the workflow. This is usually used for testing and diagnostics rather than production runs.
If none of the optional parameters are supplied, the graph is executed once by default. In this case it is assumed that the PEs at the root of the workflow execute once and determine internally as to when processing is complete.


Multiprocessing
----------------

This mapping leverages multiple processors on a shared memory system using the Python multiprocessing package. 
The user can control the number of processes used by the mapping.

To execute a dispel4py graph by using the multiprocessing mapping run the following::

    $ dispel4py multi -n <number of processes> <module> \
                [-f file containing the input dataset in JSON format] \
                [-i number of iterations] \
                [-d input data in JSON format] \
                [-a attribute] \
                [-s]

See above for use of the parameters ``-f``, ``-d`` and ``-i``.

The argument ``-s`` forces the partitioning of the graph such that subsets of nodes are wrapped and executed within the same process. The partitioning of the graph, i.e. which nodes are executed in the same process, can be specified when building the graph. By default, the root nodes in the graph (that is, nodes that have no inputs) are executed in one process, and the rest of the graph is executed in many copies distributed across the remaining processes.


MPI
-----

A dispel4py graph can also be mapped to MPI for parallel execution by any MPI implementations such as MPICH (https://www.mpich.org/) or Open MPI (http://www.open-mpi.org/).
To use the dispel4py MPI mapping mpi4py must be installed (which is wrapper for using MPI in Python).


Submitting dispel4py with MPI 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To execute a dispel4py graph by using the MPI mapping run the following::

    $ mpiexec -n <number mpi_processes> dispel4py mpi module \
        [-f file containing the input dataset in JSON format] \
        [-i number of iterations/runs] \
        [-d input data in JSON format] \
        [-a attribute] \
        [-s]

See above for use of the parameters ``-f``, ``-d`` and ``-i``.

The argument ``-s`` forces the partitioning of the graph such that subsets of nodes are wrapped and executed within the same process. The partitioning of the graph, i.e. which nodes are executed in the same process, can be specified when building the graph. By default, the root nodes in the graph (that is, nodes that have no inputs) are executed in one process, and the rest of the graph is executed in many copies distributed across the remaining processes.

For example:: 

    $ mpiexec -n 3 dispel4py mpi \
            dispel4py.examples.graph_testing.grouping_onetoall


Storm
-----

A dispel4py graph can be translated to a Storm topology to be enacted on a Storm cluster.

To use Storm, download a release from http://storm.incubator.apache.org/downloads.html and unpack it. You may want to add ``$STORM_HOME/bin`` to the PATH where ``$STORM_HOME`` is the root directory of the unpacked Storm distribution. 

    .. note :: When calling the Storm enactment as described below make sure that ``$STORM_HOME`` is defined as an environment variable.


Using the Storm submission client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From the dispel4py directory launch the Storm submission client::

    dispel4py storm -m {local,remote,create} \
              [-r resourceDir] \
              [-f file containing the input dataset in JSON format] \
              [-i number of iterations/runs] \
              [-d input data in JSON format] \
              [-a attribute] \
              [-s] \
              module [topology_name]

The ``module`` parameter is the name of the python module that creates the workflow graph. This cannot be the file name.

The parameter ``-m`` specifies the execution mode of the Storm topology:

    *local*
        Local mode, executes the graph on the local machine in Storm local mode. No installation is required. Usually used for testing before submitting a topology to a remote cluster.
    *remote*
        Submits the graph as a Storm topology to a remote cluster (with the specified ``topology_name``). This assumes the Storm client is configured for the target cluster (usually in ``~/.storm/storm.yaml``)
    *create*
        Creates a Storm topology and resources in a temporary directory. 

The graph attribute within the given module is discovered automatically or can be specified (if there is more than one graph defined, for example) by using ``-a`` with the name of the variable.
The resulting topology is assigned the id ``topology_name`` if provided, or an id is created automatically. 

The parameter ``-s`` (save) indicates that the Storm topology and resources are not deleted when the topology has been submitted to a remote cluster or execution has completed in local mode. This is useful for debugging.

Submitting dispel4py to a Storm cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following assumes the user has access to a Storm cluster, for example running on host ``storm.example.com``. 

    .. note:: The Storm client *must* have the same version as the cluster.
 
Configure the host name of the Storm cluster in ``~/.storm/storm.yaml`` as described in the Storm documentation, for example::

	nimbus.host: "storm.example.com"

To submit the topology to the remote cluster::

	$ dispel4py storm mytestgraph MyTopologyTest01 -m remote

Here, ``mytestgraph`` is the name of the Python module that creates the dispel4py graph, and ``MyTopologyTest01`` is the name that is assigned to the topology on the cluster. The name is optional and a random UUID will be assigned if it is not provided.

The topology can be monitored on the web interface of the Storm cluster.

Note that a topology runs forever until it is killed explicitly. To kill the topology on the remote cluster use the web interface or the Storm client::

	$ $STORM_HOME/bin/storm kill <topology name> -w <wait time>

where ``<wait time>`` is the time that Storm waits between deactivation and shutdown of a topology.

Testing the Storm topology in local mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To test the topology in local mode, call the Storm submission client with local mode, for example::

    $ dispel4py storm mytestgraph -m local

Note that the topology runs forever and does not shut down by itself. It can be cancelled with Ctrl-C on the commandline or by killing the JVM process.

