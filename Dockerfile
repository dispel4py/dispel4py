# Create Ubuntu environment

FROM ubuntu:14.04
MAINTAINER Amy Krause <a.krause@epcc.ed.ac.uk>

RUN apt-get update && apt-get install wget curl python-dev python-pip python-setuptools git openmpi-bin openmpi-common libopenmpi-dev -y
RUN pip install mpi4py

# install dispel4py latest
RUN pip install git+git://github.com/dispel4py/dispel4py.git@master

