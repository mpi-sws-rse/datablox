Datablox
============
Assemble big data computations from blocks.

This is still under development.

Setup
============
In order to run datablox,  ZeroMQ must be installed in the system.

datablox depends on the following python modules:

 *  zmq python bindings

Individual blox depend on several others:

 * pymongo 2.0.1 for Mongodb (also requires Mongodb to be running)
 * sunburnt 0.5 for Solr (sunburnt in turn requires httplib2 and lxml)

See the .meta file in the element's subdirectory (under blox/) for details on a given element.


Running
============

There are two main components to datablox: loader.py and care_taker.py

Before running loader or care_taker, please set the following environment variable in each shell:

export BLOXPATH=path_to_datablox/blox

Loader requires a json configuration file which specifies the topology. Some configuration files are present in examples directory. Loader takes the json configuration file and a list of ip-addresses (of nodes in which datablox can run) and distributes the elements described in the configuration among the different nodes. 

For example, to run example.json in the local machine, the command would be (assuming it is being run in the datablox project directory):

python datablox_framework/datablox_framework/loader.py examples/example.json 127.0.0.1

To run it on machines with ip-addresses 10.0.0.1, 10.0.0.2, 10.0.0.3:

python datablox_framework/datablox_framework/loader.py examples/example.json 10.0.0.1 10.0.0.2 10.0.0.3

The care_taker process must be running on each of the nodes given to loader. If the process is not running on any of the nodes, loader will wait until it is started. care-taker.py, currently, must be run in the datablox_framework directory where it is present:

cd datablox_framework/datablox_framework/
python care_taker.py

To ensure the system is setup correctly, please run the example mentioned above in the local machine. This should print all the files in the datablox_framework directory along with various attributes of the files. Note that care_taker process keeps running even after the master declares the run is complete and it is ok to quit. This is so that when you change the configuration file, you don't have to remember to start the care_taker process in each node.

Other examples to try are:

 * anagram.json - prints all anagrams of a word listed in the configuration file
 * counter.json - counts numbers
 * categorize.json - takes a list of files and categorizes them based on their file extension

Debugging
===========

The best way to test a configuration is to run it on the local machine first before deploying it to a cluster.

During the run if either the loader or any of the care_takers displays any exceptions and you want to restart, you will have to kill loader, care_takers and any elements that may be running in any of the nodes. An easy way to do that would be to kill all python processes in all the nodes with:

killall python

Note that this will kill other python processes unrelated to datablox.

BUilding and Installing with Engage
============================
If you run "make all" from the top-level directory of datablox, it
will build an Engage distribution that includes Datablox. This can
then be used to install Datablox and start the caretaker process. When
running under Engage, Datablox will automatically install any
dependent components needed by individual blocks.


Building on Ubuntu
-------------------
Here are the steps to build Datablox with Engage on Ubuntu::

  apt-get install git-core g++ ocaml zlib1g-dev python2.6-dev # engage dependencies
  git clone git://github.com/mpi-sws-rse/datablox.git
  cd ./datablox
  make all

Documentation
==============

See docs folder for the description of configuration language.
blox_meta folder contains documentation and requirements for individual elements.

Copyright 2011, MPI-SWS and genForma Corporation
