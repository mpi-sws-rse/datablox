Design
=========

The design of datablox is inspired by Click modular router. As with click, the idea is to have simple modules which compose together in interesting ways to perform various functions. The basic module is called an **element**. An element is expected to perform one well-defined task. For example, a file-crawler element can list all the files in a file-system along with their metadata or a filter element can select only certain data which satisfies a criterion etc. Each element belongs to one **element-class**, which is the blue-print describing how instances of the class should behave.

Elements are composed together through ports. An element can have any number of input and output ports. Elements communicate with one another when the output port of one element is connected to the input port of another. The communication is always initiated by the output port. Ports are typed and different ports usually have different semantics.

Data exchanged between ports is called a **log**. A log is a list of arbitrary key-value pairs, currently stored as json objects.

Ports are of two types:

1. **Push**
2. **Query**

Push output ports can only be connected to Push input ports. We call such a connection a **Push-connection**. The same applies for Query ports.

Push
    In a Push-connection, the data-flow is unidirectional: from the Push-output port to Push-input port. An example of a Push port is a file-system crawler which pushes information about each file it visits during the crawl.
    
Query
    In a **Query-connection**, the data-flow is initiated by the output port, by asking the input element a query. The input element then responds to this by sending the result over along that connection. An example of a Pull port is a database element which listens to its input port for queries on the database and returns the results.

Connections are implicitly queued. Once a queue is full, the sender is blocked until the receiver dequeues some logs. Hence, it is encouraged to send logs in batches to improve network throughput.

Elements maybe depend on external software or libraries for their functionality. All the required components are packaged together and are considered a part of the element.

A **topology** is a directed graph whose vertices are elements and edges are connections between them. Edges are always directed from the output ports to the input ports. Topologies are specified in a domain specific language (DSL) and the file containing a topology is called a **system configuration file**.

A **node** is a machine (physical or virtual) on which elements can be run. Any number of elements can be present in a node and an element can span multiple nodes (due to the external dependencies on software, services etc.).

The user is expected to give the configuration file and details about nodes to datablox framework (usually their ip-addresses). The framework then does the following:

1. Type-checks to make sure the connections are valid
2. Sets up all the nodes to be able to run elements
3. Distributes the elements among the nodes and starts running them
4. Monitors elements for crashes and loads. If an element crashes, it is restarted. Some special elements (shards) are parallelized dynamically based on loads.

Shard
    In order to dynamically parallelize processing, datablox provides a special kind of element called **shard**. A shard, like any element, has input and output ports but it relegates all its processing to other elements which belong to it. It provides the following functions:
    
    1. It gives minimum number of elements required for the shard to run
    2. If the Master (who is monitoring the loads of all the nodes) thinks that the system is better served by parallelizing the shard (by adding more elements), it will ask the shard whether it can add an element. In reply, the shard can say no, or it can give a cost associated with the addition. The cost indicates the effort required to shirk it back if the new node needs to be deallocated. For example, it costs more to shard a database which holds a lot of state than a file-categorizer which holds no state.
    3. Once a new element has been added, it takes care of any migration that need to happen to initialize it.
    4. If it gets a log from a Push port, it forwards it to one or more elements for processing. Similarly, if it gets a Pull query, it requests one or more elements for results, aggregates them and returns the aggregate.

Architecture
=============

The datablox system consists of two main classes: **Master** and **Care-taker**. The master is responsible for coordinating all the 4 tasks that the framework performs (mentioned above) with the help of Care-taker processes running on each node under the system. On each node, the Care-taker process listens to the requests from Master and takes the following actions:

- It runs elements on a node when requested by getting the configuration parameters from the Master
- It measures the load on the node and returns this data when requested by the Master
- It removes elements which are not running and sometimes force-quits running elements if Master requests it

Implementation
===============

The framework is written in python and the element-classes should provide python interfaces. The connection queues among elements are maintained through ZeroMQ. Setting up of nodes and initial deployment is handled through Engage.

Pending implementation tasks:

- Restarting failed nodes
- Initial deployment through Engage
- Ability to install software dependencies of elements through Engage
- Better heuristics to parallelize shards
- Port type checker