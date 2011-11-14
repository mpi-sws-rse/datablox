Datablox
============
Assemble big data computations from blocks.

This is still under development.

Setup
============

See "setup" for instructions.

Running
============

loader.py must know where the blox are located. For this to happen, please set the following environment variable:

export BLOXPATH=path_to_datablox/blox

loader.py requires a json configuration file which specifies the system. Some configuration files are present in examples directory. To ensure the system is setup correctly, please run:

python datablox_framework/datablox_framework/loader.py examples/example.json

This should print all the files in the datablox directory along with various attributes of the files.


Documentation
==============

See docs folder for the description of configuration language.
blox_meta folder contains documentation and requirements for individual elements.

Copyright 2011, MPI-SWS and genForma Corporation
