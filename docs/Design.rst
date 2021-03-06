Architecture
=========

The design of datablox is inspired by Click modular router. As with Click, the idea is to have simple modules which compose together in interesting ways to perform various functions. The basic module is called an **block** and it usually performs one well-defined task. For example, a file-crawler block can list all the files in a file-system along with their metadata or a filter block can select only certain data which satisfy a criterion. Each block belongs to one **block-class**, which is the blue-print describing how instances of the class should behave.

Blocks are composed together through ports. A block can have any number of input and output ports. Blocks communicate with one another when the output port of one block is connected to the input port of another. The communication is always initiated by the output port. Ports are typed and different ports usually have different semantics.

Data exchanged between ports is called a **log**. A log is a list of arbitrary key-value pairs.

Ports are of two types:

1. **Push**
2. **Query**

Push output ports can only be connected to Push input ports. We call such a connection a **Push-connection**. The same applies for Query ports.

Push
    In a Push-connection, the data-flow is unidirectional: from the Push-output port to Push-input port. An example of a Push port is a file-system crawler which pushes information about each file it visits during the crawl.
    
Query
    In a **Query-connection**, the data-flow is initiated by the output port, by asking the input block a query. The input block then responds to this by sending the result over along that connection. An example of a Pull port is a database block which listens to its input port for queries on the database and returns the results.

Connections are implicitly queued. Once a queue is full, the sender is blocked until the receiver dequeues some logs. Hence, it is encouraged to send logs in batches to improve network throughput.

Blocks maybe depend on external software or libraries for their functionality. All the required components are packaged together and are considered a part of the block.

A **topology** is a directed graph whose vertices are blocks and edges are connections between them. Edges are always directed from the output ports to the input ports. Topologies are specified in a domain specific language (DSL) and the file containing a topology is called a **system configuration file**.

A **node** is a machine (physical or virtual) on which blocks can be run. Any number of blocks can be present in a node and a block can span multiple nodes (due to the external dependencies on software, services etc.).

The user is expected to give the configuration file and details about nodes to datablox framework (usually their ip-addresses). The framework then does the following:

1. Type-checks to make sure the connections are valid
2. Sets up all the nodes to be able to run blocks
3. Distributes the blocks among the nodes and starts running them
4. Monitors blocks for crashes and loads. If a block crashes, it is restarted. Some special blocks (shards) are parallelized dynamically based on loads.

Shard
    In order to dynamically parallelize processing, datablox provides a special kind of block called **shard**. A shard, like any block, has input and output ports but it relegates all its processing to other blocks which belong to it. It provides the following functions:
    
    1. It gives minimum number of blocks required for the shard to run
    2. If the Master (who is monitoring the loads of all the nodes) thinks that the system is better served by parallelizing the shard (by adding more blocks), it will ask the shard whether it can add a block. In reply, the shard can say no, or it can give a cost associated with the addition. The cost indicates the effort required to shirk it back if the new node needs to be deallocated. For example, it costs more to shard a database which holds a lot of state than a file-categorizer which holds no state.
    3. Once a new block has been added, it takes care of any migration that need to happen to initialize it.
    4. If it gets a log from a Push port, it forwards it to one or more blocks for processing. Similarly, if it gets a Pull query, it requests one or more blocks for results, aggregates them and returns the aggregate.

Implementation
=============

The datablox system consists of two main runtime units: **Master** and **Care-taker**. The master is responsible for coordinating all the 4 tasks that the framework performs (mentioned above) with the help of Care-taker processes running on each node under the system. On each node, the Care-taker process listens to the requests from Master and takes the following actions:

- It runs blocks on a node when requested by getting the configuration parameters from the Master
- It measures the load on the node and returns this data when requested by the Master
- It removes blocks which are not running and sometimes force-quits running blocks if Master requests it

Logs are currently stored as json objects.

Implementation Details
===============

The framework is written in python and the block-classes should provide python interfaces. The connection queues among blocks are maintained through ZeroMQ. Setting up of nodes and initial deployment is handled through Engage.

Pending implementation tasks:

- Restarting failed nodes
- Initial deployment through Engage
- Ability to install software dependencies of blocks through Engage
- Better heuristics to parallelize shards
- Port type checker

Transferring large files
==============

Large files are not transferred through ZeroMQ. They are managed out-of-band through HTTP servers.

The care-taker program on every node hosts a HTTP server just for serving files. So if a block A on node N1 is sending a file (F) to block B on node N2, it encrypts the path of F with N1's key to get enc(path). We use symmetric key encryption for it. It sends enc(path) over to B. When B wants to process the contents of F, it requests the HTTP file server on N1 to return the contents on enc(path). File server get enc(path), decrypts it with N1's key to get path and returns the contents through HTTP.

This process has several advantages:

If B does not need to process the file contents but is only looking at other fields, Datablox will not transfer the file contents to B. If some block K eventually in the topology ends up reading the file, the file is only sent to K and does not need to pass through B, C etc.
A node can revoke privileges for file access to blocks by changing its key. That way the HTTP file server will no longer be able to decrypt the paths anymore.
A block in another node cannot request arbitrary files from the file server because it will not know the private key for encrypting the paths.
The additional complications introduced by this approach are:

Block A must somehow make sure the file F is present on the disk at that path until the block which is supposed to use the file has accessed it. The file server currently does not communicate with blocks about which files have been accessed. So A must have a mechanism to get tokens from other blocks when file access is complete. We should probably find a more general solution for it.