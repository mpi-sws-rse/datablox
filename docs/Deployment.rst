Datablox Deployment
==============================

Datablox topologies are deployed via Engage (http://github.com/genforma/engage). Each blox type has an associated Engage *driver* that can deploy and manage the software for the block and its dependent components.


Packaging
------------------
The datablox-specific extensions to Engage are organized as an Engage *extension* under ``datablox/engage/datablox``.  Each block has a an associated driver under ``datablox/engage/datablox/drivers/<blockname__blockversion>``. There is associated metadata describing the block and its dependencies either in the same directory as the driver or in
``datablox/engage/datablox/metadata``.

The build process packages up the associated block code for each driver (located at ``datablox/blox/<blockname_blockversion>``) as a gzipped tar archive and places it under ``datablox/engage/blox/sw_packages``. At deployment time, the driver will extract this archive
as a subdirectory under ``$BLOXPATH``.

Runtime
-----------
The datablox *Manager* process interacts with the Engage *deployer*  to setup the individual nodes.
Given a topology, the manager determines the mapping of blox to nodes and creates an Engage
*partial install spec* for each node and hands it to the deployer. For each node, the deployer
provisions the node (if necessary), bootstraps the Engage environment on that node, deploys
the requested components, starts the caretaker process, and returns a *full install spec* describing the deployed configuration back to the manager.

Note that individual blox may require the deployment of additional nodes. For example, a MongoDb
block may request to be deployed across four nodes (with MongoDb handling the sharding). This would be handled transparently by Engage.

After the initial deployment, the datablox manager can call the deployer to add or remove nodes. Going
forward, we will add the capability to add/remove individual blox on existing nodes.


Organization of a deployed node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Each node that runs blox will have an Engage *deployment home*.  The drivers for datablox are installed with the rest of the Engage infrastructure under ``<deployment_home>/engage``.  The datablox
framework code is installed as a python package under ``<deployment_home>/python``. The individual blox are installed at ``<deployment_home>/blox/<blockname_blockversion>``.


Open issues
------------------
 * Need to work out details of boundry between manager and deployer
 * If a block is deployed as a multi-node component by Engage, can the manager increase/decrease the number of nodes allocated to the bock?
