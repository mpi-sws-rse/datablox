Datablox Deployment
==============================

Datablox topologies are deployed via Engage (http://github.com/genforma/engage). Each blox type has an associated Engage *driver* that can deploy and manage the software for the block and its dependent components. The datablox-specific extensions to Engage are organized as an Engage *extension* under ``datablox/engage/datablox``.  


Block Packaging
------------------
Each block has a an associated driver under ``datablox/engage/datablox/drivers/<blockname__blockversion>``. There is associated metadata describing the block and its dependencies either in the same directory as the driver or in
``datablox/engage/datablox/metadata``.

The build process packages up the associated block code for each driver (located at ``datablox/blox/<blockname_blockversion>``) as a gzipped tar archive and places it under ``datablox/engage/blox/sw_packages``. At deployment time, the driver will extract this archive
as a subdirectory under ``$BLOXPATH``.

Datablox Engage Adapter
--------------------------------
The Datablox Engage Adapter is a Python package distribution located in the source tree at
``datablox/engage/adapter_pkg``. This adapter is included in the the Engage extension and
is installed as a part of the Datablox caretaker installation.  The adapter provides for the
Datablox framework an interface to Engage's deployer and other functionality.

The presence of this package (specifically the ``datablox_engage_adapter`` package) is used to
by the Datablox loader to determine that Datablox was deployed via Engage. Otherwise, it is assumed
to be running in standalone mode.


Runtime
-----------
A Datablox node is initially deployed with just the caretaker and its dependencies. When the caretaker
process requests that a block be loaded, it first makes a call to the Datablox Engage Adapter to
deploy that block and its dependencies into the local environment.

Organization of a deployed node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Each node that runs blox will have an Engage *deployment home*.  The drivers for datablox are installed with the rest of the Engage infrastructure under ``<deployment_home>/engage``.  The datablox
framework code is installed as a python package under ``<deployment_home>/python``. The individual blox are installed at ``<deployment_home>/blox/<blockname_blockversion>``.


Open issues
------------------
 * Need to add details of multi-node support
