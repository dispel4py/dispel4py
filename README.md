dispel4py
=========

dispel4py is a Python library used to describe abstract workflows for distributed data-intensive applications. These workflows are compositions of processing elements representing knowledge discovery activities (such as batch database querying, noise filtering and data aggregation) through which significant volumes of data can be streamed in order to manufacture a useful knowledge artefact. Such processing elements may themselves be defined by compositions of other, more fundamental computational elements, in essence having their own internal workflows. Users can construct workflows importing existing processing elements from a registry, or can define their own, recording them in a registry for later use by themselves or others.

Abstract dataflows described in dispel4py can be executed in numerous environments, for example using a Storm cluster or as an MPI job. Thus dispel4py allows to construct workflows without particular knowledge of the specific context in which they are to be executed, granting them greater generic applicability.

Dependencies 
------------

dispel4py has been tested with Python versions *2.7.5*, *2.7.2* and *2.6.6*.

The following Python packages are required to run dispel4py:

- networkx (https://networkx.github.io/)

If using the MPI mapping:

- mpi4py (http://mpi4py.scipy.org/)

If using the Storm mapping:

- Python Storm module, available here: https://github.com/apache/storm/tree/master/storm-multilang/python/src/main/resources/resources, to be placed in directory `resources`.
- Python Storm thrift generated code, available here: https://github.com/apache/storm/tree/master/storm-core/src/py

Installation
------------

The easiest way to install dispel4py is via pip (https://pypi.python.org/pypi/pip):

`pip install dispel4py`

Or, if you have git installed, you can install the latest version directly from github:

`pip install git+git://github.com/dispel4py/dispel4py.git@master`

Alternatively, download the ZIP or clone this repository to your desktop. You can then install from the local copy to your python environment by calling:

`python setup.py install`

from the dispel4py root directory.

Docker image
------------

A Docker image with the latest dispel4py installation is available from the Docker Hub: https://registry.hub.docker.com/u/akrause2014/dispel4py/

Documentation
-------------

The wiki documentation explains how to install and test dispel4py: https://github.com/dispel4py/dispel4py/wiki

dispel4py user documentation: http://dispel4py.org/documentation

The dispel4py website is http://dispel4py.org

Travis CI
---------

[![Build Status](https://travis-ci.org/dispel4py/dispel4py.svg)](https://travis-ci.org/dispel4py/dispel4py)
https://travis-ci.org/dispel4py/dispel4py


![Logo](http://www2.epcc.ed.ac.uk/~amrey/VERCE/Dispel4Py/_static/DISPEL4PY_web_trans.png)

