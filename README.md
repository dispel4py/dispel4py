dispel4py
=========

dispel4py is a free and open-source Python library for describing abstract stream-based workflows for distributed data-intensive applications. It enables users to focus on their scientific methods, avoiding distracting details and retaining flexibility over the computing infrastructure they use.  It delivers mappings to diverse computing infrastructures, including cloud technologies, HPC architectures and  specialised data-intensive machines, to move seamlessly into production with large-scale data loads. The dispel4py system maps workflows dynamically onto multiple enactment systems, such as MPI, STORM and Multiprocessing, without users having to modify their workflows.

Dependencies 
------------

dispel4py has been tested with Python *2.7.6*, *2.7.5*, *2.7.2*, *2.6.6* and Python *3.4.3*.

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

Or, if you have git installed, you can install the latest development version directly from github:

`pip install git+git://github.com/dispel4py/dispel4py.git@master`

Alternatively, download the ZIP or clone this repository to your desktop. You can then install from the local copy to your python environment by calling:

`python setup.py install`

from the dispel4py root directory.

Docker image
------------

A Docker image with the latest dispel4py development version, based on Ubuntu 14.04 with OpenMPI, is available from the Docker Hub. For more details see: https://registry.hub.docker.com/u/dispel4py/dispel4py/

The dispel4py image is deployed as follows:

`docker pull dispel4py/dispel4py`

Documentation
-------------

The wiki documentation explains how to install and test dispel4py: https://github.com/dispel4py/dispel4py/wiki

[![Build Status](https://travis-ci.org/dispel4py/dispel4py.svg)](https://travis-ci.org/dispel4py/dispel4py)
[![PyPI version](https://badge.fury.io/py/dispel4py.svg)](http://badge.fury.io/py/dispel4py)
[![Coverage Status](https://coveralls.io/repos/dispel4py/dispel4py/badge.svg?branch=master)](https://coveralls.io/r/dispel4py/dispel4py?branch=master)


