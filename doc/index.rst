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
   enactment
   tools
   api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Acknowledgements
----------------

The current development of dispel4py is supported by the VERCE project, http://www.verce.eu (EU RI 283543) where it has been used for processing and correlating data streams from digital seismometer networks, for preparing data for modelling the forward propagation of seismic waves, the post-processing of simulation results and analysis of "misfit", the relationship between simulation and observation data streams.
It builds on the work of previous projects, including: the EU ADMIRE project, http://www.admire-project.eu (EU ICT 215024), the EPSRC projects the e-Science Core Programme Senior Research Fellow (EP/D079829/1) and 
the National e-Science Centre Research Platform, research.nesc.ac.uk, (EP/F057695/1), 
the NERC EFFORT project, http://www.geos.ed.ac.uk/sidecar/geohazards/research-hubs/hazards/seismology/time-dependent-earthquake-hazard, (NE/H02297X/1) and the NERC Terra-correlator project, https://www.wiki.ed.ac.uk/display/Terra/Terra-correlator+wiki, (NE/L012979/1),  
the NSF Open Science Data Cloud (OSDC) PIRE Project, http://pire.opensciencedatacloud.org, 
and the Ministry of Education Malaysia grants (FRGS FP051-2013A and UMRG RP001F-13ICT).

