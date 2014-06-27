Enactment of Dispel4Py workflows
================================

A workflow must be submitted for execution over available resources in order to produce results. 

Storm
-----

A Dispel4Py graph can be translated to a Storm topology to be enacted on a Storm cluster.

To use a Storm client, download a release from http://storm.incubator.apache.org/downloads.html and unpack it. You may want to add ``$STORM_HOME/bin`` to the PATH where ``$STORM_HOME`` is the root directory of the unpacked Storm distribution.

Creating a Storm topology
^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Storm topology from a Dispel4Py workflow and copy all required resources into a temporary directory run the following::

	$ python create_resources.py <module> <graph var>

where the first argument ``<module>`` is the name of the python module (**without the file extension .py**) that creates a workflow graph, and the second argument ``<graph var>`` is the name of the variable that contains the graph object. For example::

	$ python create_resources.py test.workflow_client graph

The last line of output from this script (if there are no errors) is the path of the temporary directory ``<temp dir>`` where the generated topology and all required resources are stored.

Submitting Dispel4Py to a Storm cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following assumes the user has access to a Storm cluster, for example running on host ``storm.example.com``. 

    .. note:: The Storm client *must* have the same version as the cluster.
 
Configure the host name of the Storm cluster in ``~/.storm/storm.yaml`` as described in the Storm documentation, for example::

	nimbus.host: "storm.example.com"

To submit the topology to the remote cluster, change to the temporary directory created above and execute ``storm shell``::

	$ cd <temp dir>
	$ $STORM_HOME/bin/storm shell resources/ python storm_submission_client.py <topology name>

The last argument ``<topology name>`` is the name of the topology as it is submitted to the Storm cluster. This is an arbitrary name that you can choose but only one topology with a given name can run at a time. Do not change any of the other arguments.

A topology runs forever until it is killed explicitly. To kill the topology on the remote cluster::

	$ $STORM_HOME/bin/storm kill <topology name> -w <wait time>

where ``<wait time>`` is the time that Storm waits between deactivation and shutdown of a topology.

Testing the Storm topology in local mode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To test the topology in local mode, i.e. to run it on the client machine, change to the temporary directory to compile and run the Java client ``dispel4py.storm.ThriftSubmit``. Make sure that the Storm distribution jars are on the classpath and additionally the directory ``<temp dir>/resources/`` when running the client. For example::

    $ cd <temp dir>
    $ javac -cp .:$STORM_HOME/lib/*:$STORM_HOME/storm-0.8.2.jar dispel4py/storm/ThriftSubmit.java
    $ java -cp .:$STORM_HOME/lib/*:$STORM_HOME/storm-0.8.2.jar:./resources/ dispel4py.storm.ThriftSubmit topology.thrift <topology name>

The last argument ``<topology name>`` is the name of the topology. Do not change any of the other arguments.

Note that the topology runs forever and does not shut down by itself. It can be cancelled with Ctrl-C on the commandline or by killing the JVM process.

MPI
-----


