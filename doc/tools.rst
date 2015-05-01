Dispel4Py Tools
---------------


IPython extension
=================

.. The IPython extension for Dispel4Py allows to run the commands described above within the interactive web environment of IPython. Using an interactive IPython notebook in your browser load the Dispel4Py extension package::
..
..     %load_ext dispel4py_extension
    
.. You can now list package contents in the registry, view the source of Dispel4Py components or register new Dispel4Py components using the ``%dispel4py`` command from the notebook::
..
..     %dispel4py list dispel4py.test
..     %dispel4py view eu.verce.seismo.Detrend_CM
    
The dispel4py IPython extension supports the display of the workflow graph with a layout created by the open source graph visualisation software *Graphviz dot* (http://www.graphviz.org/). If the commandline tool *dot* is installed and available on the IPython notebook server the graph of a dispel4py workflow can be displayed like this::

    from dispel4py_extension import display
    display(graph)

.. image:: images/dispel4py_graph.png