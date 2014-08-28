Enactment of Dispel4Py workflows
================================

A workflow must be submitted for execution over available resources in order to produce results. 
Dispel4Py provides various mappings for a number of enactment engines.


Sequential Processing
---------------------

This mapping serves as an enactment engine for processing small datasets, typically during development or when testing a new installation.
The processing elements of the graph are executed in sequence by a single process.

To execute a Dispel4py graph by using the simple (sequential) mapping run the following::

    $ python -m dispel4py.simple_process <module> [-f file containing the input dataset in JSON format] [-i number of iterations]

If the number of iterations is not indicated, the graph is executed once by default.
If an input file is specified with ``-f`` then the parameter ``-i`` will be ignored.

Multiprocessing
----------------

This mapping leverages multiple processors on a shared memory system using the Python multiprocessing package. 
The user can control the number of processes used by the mapping.

To execute a Dispel4py graph by using the multiprocessing mapping run the following::

    $ python -m dispel4py.multi_process -n <number of processes> <module> [-f file containing the input dataset in JSON format] [-i number of iterations] [-s]
    
If ``-s`` is specified the graph is partitioned to execute several PEs within one process.

MPI
-----

A Dispel4Py graph can also be mapped to MPI for executing in parallel by any of MPI implementations as mpich2 or openmpi.
To use Dispel4py + MPI is needed to have installed mpi4py (which is wrapper for using MPI in python) and mpich2 or openmpi (which are MPI interfaces).

Installing mpi4py and openmpi
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
For installing openmpi and mpi4py follow the next steps.

    .. note:: These steps can be different depending on the host operating system. Those are for Mac OS X.

Install openmpi::
	
    $ sudo por install openmpi
    $ mkdir ~/src
    $ cd ~/src
    $ cd /Users/xxx/Downloads/openmpi-1.6.5.tar.gz .	 	
    $ tar zxvf openmpi-1.6.5.tar.gz 
    $ cd openmpi-1.6.5
    $ ./configure --prefix=/usr/local
    $  make all  (This step take a while) 
    $ sudo make install
 
Important: 

    .. note:: Check if ``/usr/local/bin`` is in your path (echo $PATH). If you do not see ``/usr/local/bin`` listed between the colons, you will need to add it. ( echo export PATH=/usr/local/bin:$PATH' >> ~/.bash_profile )  	


Install mpi4py::

    $ sudo easy_install mpi4py


Submitting Dispel4Py with MPI 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To execute a Dispel4py graph by using the MPI mapping run the following::

    $ mpiexec -n <number mpi_processes> python -m dispel4py.worker_mpi module [-f file containing the input dataset in JSON format] [-i number of iterations/runs] [-s]

If the number of iterations is not indicated, the graph is executed once by default.
If an input file is specified with ``-f`` then the parameter ``-i`` will be ignored.

The argument ``-s`` forces to run the graph in simple processing mode, which means that a number of nodes can be wrapped and executed within the same process. The partitioning of the graph, i.e. which nodes are executed in the same process, can be specified when building the graph. By default, the root nodes in the graph (that is, nodes that have no inputs) are executed in one process, and the rest of the graph is executed in many copies distributed across the remaining processes.

For example:: 
    
    $ mpiexec -n 3 python -m dispel4py.worker_mpi test.graph_testing.grouping_onetoall 
        

Storm
-----

A Dispel4Py graph can be translated to a Storm topology to be enacted on a Storm cluster.

To use Storm, download a release from http://storm.incubator.apache.org/downloads.html and unpack it. You may want to add ``$STORM_HOME/bin`` to the PATH where ``$STORM_HOME`` is the root directory of the unpacked Storm distribution. 

    .. note :: When calling the Storm enactment as described below make sure that ``$STORM_HOME`` is defined as an environment variable.


Using the Storm submission client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From the dispy directory launch the Storm submission client::

    python -m dispel4py.storm.storm_submission -m {local,remote,create} [-r resourceDir] [-a attribute] [-s] module [name]

where ``module`` is the name of the python module (**without the file extension .py**) that creates a workflow graph. The parameter ``-m`` specifies the execution mode of the Storm topology:
    *local*
        Local mode, executes the graph on the local machine in Storm local mode. No installation is required. Usually used for testing before submitting a topology to a remote cluster.
    *remote*
        Submits the graph as a Storm topology to a remote cluster. This assumes the Storm client is configured for the target cluster (usually in ``~/.storm/storm.yaml``)
    *create*
        Creates a Storm topology and resources in a temporary directory. 

The graph attribute within the given module is discovered automatically or can be specified (if there is more than one graph defined, for example) by using ``-a`` with the name of the variable.
The resulting topology is assigned the id ``name`` if provided, or an id is created automatically. 
If using ``-s`` (save) the Storm topology and resources are not deleted when the topology has been submitted or completed execution in local mode. This is useful for debugging.

Submitting Dispel4Py to a Storm cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following assumes the user has access to a Storm cluster, for example running on host ``storm.example.com``. 

    .. note:: The Storm client *must* have the same version as the cluster.
 
Configure the host name of the Storm cluster in ``~/.storm/storm.yaml`` as described in the Storm documentation, for example::

	nimbus.host: "storm.example.com"

To submit the topology to the remote cluster::

	$ python -m dispel4py.storm.storm_submission mytestgraph MyTopologyTest01 -m remote

Here, ``mytestgraph`` is the name of the Python module that creates the Dispel4Py graph, and ``MyTopologyTest01`` is the name that is assigned to the topology on the cluster. The name is optional and a random UUID will be assigned if it is not provided.

The topology can be monitored on the web interface of the Storm cluster.

Note that a topology runs forever until it is killed explicitly. To kill the topology on the remote cluster use the web interface or the Storm client::

	$ $STORM_HOME/bin/storm kill <topology name> -w <wait time>

where ``<wait time>`` is the time that Storm waits between deactivation and shutdown of a topology.

Testing the Storm topology in local mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To test the topology in local mode, call the Storm submission client with local mode, for example::

    $ python -m dispel4py.storm.storm_submission mytestgraph -m local

Note that the topology runs forever and does not shut down by itself. It can be cancelled with Ctrl-C on the commandline or by killing the JVM process.

