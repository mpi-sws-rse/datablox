Datablox
============
Assemble big data computations from blocks.

This is still under development.

Setup
============
In order to run datablox,  ZeroMQ must be installed in the system.

datablox depends on the following python modules:

 *  zmq python bindings
 *  Engage

Individual blox depend on several others:

 * pymongo 2.0.1 for Mongodb
 * sunburnt 0.5 for Solr (sunburnt in turn requires httplib2 and lxml)

See the .meta file in the block's subdirectory (under blox/) for details on a given block.


Running
============

There are two main components to datablox: loader.py and care_taker.py

Before running loader or care_taker, please set the following environment variable in each shell:

export BLOXPATH=path_to_datablox/blox

Loader requires a json configuration file which specifies the topology. Some configuration files are present in examples directory. Loader takes the json configuration file and a list of ip-addresses (of nodes in which datablox can run) and distributes the blocks described in the configuration among the different nodes. 

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

During the run if either the loader or any of the care_takers displays any exceptions and you want to restart, you will have to kill loader, care_takers and any blocks that may be running in any of the nodes. An easy way to do that would be to kill all python processes in all the nodes with:

killall python

Note that this will kill other python processes unrelated to datablox.

Building and Installing with Engage
====================================
Datablox can be installed  via the Engage deployment platform (http://github.com/genforma/engage). 
This is accomplished by
building an Engage distribution that includes Datablox as an extension. Engage can then install the
Datablox framework and start the caretaker process. When
running under Engage, Datablox will automatically install any
dependent components needed by individual blocks.


Building on Ubuntu
-------------------
Here are the steps to build Datablox with Engage on Ubuntu::

  sudo apt-get update
  # engage dependencies
  sudo apt-get install git-core g++ ocaml zlib1g-dev python-dev
  sudo apt-get install python-crypto python-virtualenv make libzmq-dev libzmq1
  sudo pip install pyzmq
  git clone git://github.com/mpi-sws-rse/datablox.git
  cd ./datablox
  make all

Building on MacOSX
-------------------------
Datablox is currently supported on MacOSX 10.5 and 10.6.  As
prerequisites, you need to have the following software installed on
your mac:

 * Python 2.6 or 2.7
 * Apple's XCode (to get g++)
 * OCaml (http://caml.inria.fr)
 * MacPorts (http://www.macports.org)
 * ZeroMQ (http://www.zeromq.org)
 * The following Python packages:

   * virtualenv (http://pypi.python.org/pypi/virtualenv)
   * setuptools (http://pypi.python.org/pypi/setuptools)
   * pycrypto (http://pypi.python.org/pypi/pycrypto)
   * pyzmq (http://pypi.python.org/pypi/pyzmq)

If you are running MacOSX 10.5 (Leopard), the version of Python included with the OS is too old, and
you will have to install a separate local copy of Python 2.6 or Python 2.7. Either way, we recommend installing
MacPorts and using the MacPorts Python package (`python27 <https://trac.macports.org/browser/trunk/dports/lang/python27/Portfile>`_).

If you use MacPorts, you can get most of the dependencies set up with minimal pain by installing the associated ports: `py27-crypto <https://trac.macports.org/browser/trunk/dports/python/py27-crypto/Portfile>`_,
`zmq <https://trac.macports.org/browser/trunk/dports/sysutils/zmq/Portfile>`_,
`py27-zmq <https://trac.macports.org/browser/trunk/dports/python/py-zmq/Portfile>`_,
`py27-virtualenv <https://trac.macports.org/browser/trunk/dports/python/py-virtualenv/Portfile>`_,  and `ocaml <https://trac.macports.org/browser/trunk/dports/lang/ocaml/Portfile>`_.

With the prerequisites installed, you can now build as follows::

  git clone git://github.com/mpi-sws-rse/datablox.git
  cd ./datablox
  make all

Testing
------------
If you wish to test datablox after building it, you can do so by running the following::

  cd ./datablox
  make test

This will install Datablox to ``~/apps``, run an example topology (``datablox/examples/file_map_reduce.json``),
and then shut down the Datablox caretaker.


Installing
-----------
Assuming you start in the directory above your
Datablox source tree and have already built it, the following will
install Datablox::

  cd ./datablox/engage/engage-dist
  ./install_datablox.py <deployment_home>

where ``<deployment_home>`` is the target directory for your
installation. If you are not running as root and your user requires a
password to run ``sudo``, then you will be asked for the sudo password.
Root access is needed to install some of the
components (e.g. zeromq). The Datablox master script will be installed
to ``<deployment_home>/python/bin/datablox-master``. You can run it as
follows::

  <deployment_home>/python/bin/datablox-master <script_name> master

where ``<script_name>`` is the Datablox topology JSON file you wish to run.

The installation will also start the Datablox *caretaker* process. To
start and stop it, you can use Engage's ``svcctl`` utility. To do
this, run::

  <deployment_home>/engage/bin/svcctl <command>

where ``<command>`` is one of: ``start``, ``stop``, or ``status``. 


Installing Worker Nodes
--------------------------
To run a multinode configuration, one first installs the *master* node
as described above. Next, we add *worker* nodes as follows:

  1. Ensure that the worker nodes have the prerequisites for Datablox installed. These prerequisites include all the packages listed above to build Datablox, except for the C++ and Ocaml compilers.
  2. Make sure the worker nodes are accessible via ``ssh`` without requiring a password. This can be accomplished by adding the master node's public ssh key (usually at ``~/.ssh/id_rsa.pub``) to the worker node's authorized keys file (usually at ``~/.ssh/authorized_keys``).
  3. Add the worker nodes to the node database maintained by the master using the ``djmctl`` utility's ``add-node`` command. See below for details.
  4. When you run ``datablox-master``, add the names you gave to the worker nodes in step 3 to the list of nodes on the command line. For example, if Datablox is installed at ``~/apps`` and we want to run the topology ``test.json`` on the nodes ``master``, ``worker1``, and ``worker2``, use: ``~/apps/python/bin/datablox-master test.json master worker1 worker2``

Adding Nodes Via djmctl
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``djmctl`` utility, installed at ``<deployment_home>/python/bin/djmctl`` is used to manage the database of nodes maintained by Datablox.  The syntax for the ``add-node`` command is::

  djmctl add-node [options] [name]

where the relevant options are::

    -h, --help            show help message and exit
    --debug               If specified, print debug information to the console
    --hostname=HOSTNAME   Public hostname of the server
    --public-ip=PUBLIC_IP
                          Public ip address of the server
    --private-ip=PRIVATE_IP
                          Private ip address of the server
    --os-user=OS_USER     OS user for node (defaults to the current user)
    --bootstrap           If specified, setup the DJM worker on the node (default behavior)
    --no-bootstrap    If specified, do not setup the DJM worker on the node.
    --no-check-for-private-ip
                          If specified, do not try to look for a private ip
                          address for this node

The name defines a unique handle used to refer to the node (e.g. on the ``datablox-master`` command
line). If not provided, the hostname, public ip, or private ip will be used.
The options ``--hostname``, ``--public-ip``, and ``--private-ip`` define ways to contact the
machine. At least one of these must be provided. A private ip (local network) address is preferred. If
you do not provide one, Datablox will try to find one, and if the machine can be reached by that address,
add it to the node's database entry. To suppress this automatic check, use the
``--no-check-for-private-ip`` option.

Here is an example, where we are adding the node ``test.genforma.com``, to be referred as ``test``::

  djmctl add-node --hostname=test.genforma.com --osuser=datablox test

The ``djmctl`` utility is part of the *Distributed Job Manager*. More details may be found at https://github.com/genforma/dist_job_mgr.

Re-initializing Worker Nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After a multinode run, the worker nodes are left with Datablox and any block dependencies installed.
By default, subsequent runs using these workers will reuse the original Datablox install. To force a
reinstallation of Datablox (and block dependencies), use the ``--always-reinstall-workers`` option.



Additional Documentation
=============================

See docs folder for the description of configuration language.
blox_meta folder contains documentation and requirements for individual blocks.

Copyright 2011, 2012 by MPI-SWS and genForma Corporation
