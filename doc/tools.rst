Dispel4Py Tools
---------------

Commandline Dispel4Py
=====================

Commandline tools are available for accessing the registry and viewing the source of registered Dispel4Py components.
The ``dispel4py`` command must be configured with the location of the registry. 
The configuration file is located in ``~/.dispel4py/config.json`` or within the current directory at ``.dispel4py/config.json``, providing the URL of the registry and the user name and workspace.

To list the available components in a package::
    
    $ ./dispel4py list dispel4py.test
    Packages:
       dispel4py.test
    Processing Elements:
      Filter
      RandomWordProducer
    Functions:
      TestFunction1
      
View the source of a Dispel4Py PE::

    $ ./dispel4py view dispel4py.test.MyTestPE
    
    from dispel4py.GenericPE import GenericPE, NAME
    class MyTestPE(GenericPE):
        def __init__(self):
            GenericPE.__init__(self)
            self.outputconnections = { 'output' :  { NAME : 'output' } }
        def process(inputs):
            return { 'output' : 'Hello World!' }
            
Register a new component::

    $ ./dispel4py register test.myexample.MyFunction test/myexample.py


IPython extension
=================

The IPython extension for Dispel4Py allows to run the commands described above within the interactive web environment of IPython. Using an interactive IPython notebook in your browser load the Dispel4Py extension package::

    %load_ext dispel4py_extension
    
You can now list package contents in the registry, view the source of Dispel4Py components or register new Dispel4Py components using the ``%dispel4py`` command from the notebook::

    %dispel4py list dispel4py.test
    %dispel4py view dispel4py.MyTestPE
    
The Dispel4Py extension supports the display of the workflow graph with a layout created by dot. If the commandline tool *Graphviz dot* (http://www.graphviz.org/) is available on the IPython notebook server a workflow graph can be displayed::

    from dispel4py_extension import display
    display(graph)

.. image:: images/dispel4py_graph.png