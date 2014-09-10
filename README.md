Dispel4Py
=========

Dispel4Py is a Python library used to describe abstract workflows for distributed data-intensive applications. These workflows are compositions of processing elements representing knowledge discovery activities (such as batch database querying, noise filtering and data aggregation) through which significant volumes of data can be streamed in order to manufacture a useful knowledge artefact. Such processing elements may themselves be defined by compositions of other, more fundamental computational elements, in essence having their own internal workflows. Users can construct workflows importing existing processing elements from a registry, or can define their own, recording them in a registry for later use by themselves or others.

Abstract dataflows described in Dispel4Py can be executed in numerous environments, for example using a Storm cluster or as an MPI job. Thus Dispel4Py allows to construct workflows without particular knowledge of the specific context in which they are to be executed, granting them greater generic applicability.

Dependencies 
------------

Dispel4Py has been tested with Python versions *2.7.5*, *2.7.2* and *2.6.6*.

The following Python packages are required to run Dispel4Py:

- networkx (https://networkx.github.io/)

If using the MPI mapping:

- mpi4py (http://mpi4py.scipy.org/)

If using the Storm mapping:

- Python Storm module, available here: https://github.com/apache/incubator-storm/tree/master/storm-core/src/multilang/py, to be placed in directory `resources`.
- Python Storm thrift generated code, available here: https://github.com/apache/incubator-storm/tree/master/storm-core/src/py

Installation
------------

The easiest way to install dispel4py is via pip (https://pypi.python.org/pypi/pip):

`sudo pip install git+git://github.com/akrause2014/dispel4py.git#egg=dispel4py`

Alternatively, download the ZIP or clone this repository to your desktop and add it to the PYTHON_PATH.

Docker image
------------

A Docker image with the latest Dispel4Py installation is available from the Docker Hub: https://registry.hub.docker.com/u/akrause2014/dispel4py/

Documentation
-------------

The wiki documentation explains how to install and test Dispel4Py: https://github.com/akrause2014/dispel4py/wiki

Dispel4Py user documentation: http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/


![Logo](http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/_static/DISPEL4PY_web_trans.png)

