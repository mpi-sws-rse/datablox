=================
Datablox Messages
=================
This document attempts to document the key message interactions
in datablox. It is recommended that you first read the summary
documents to understand the architecture and the key processes.

Process Recap
-------------
We will first recap the main processes in Datablox.

Master
  There is one master process. This process reads the topology file,
  coordinates the startup with the caretakers and blocks, gathers
  performance data during the run, and coordinates the shutdown
  (either normally or due to an error or cancel request).

Caretaker
  There is one caretaker per node (including the master node). The
  caretaker receives requests from the master. It starts blocks,
  monitors the liveness and progress of blocks, reports performance
  data back to the master, and kills blocks in the case of a
  cancel from the master.

Block
  Blocks are the components of the actual workflow. Each block is a
  separate Python process. When requested by the master, the caretaker
  writes the block's configuration to a file and then fork/execs the
  process. Blocks then communicate directly with the other blocks
  in the workflow.


Startup Messages
----------------
The master first reads the topology file and then (sequentially) starts
each block in the toplogy. The message sequence chart below shows the
sequence of messages:

.. figure:: block_initialization.svg

The master first connects to the caretaker for the node associated with
the block to be started. [#]_ It then sends an ``ADD BLOCK`` message with
the configuration for the block. This configuration includes the
parameters specified in the topology file as well as the network
addresses of the input and output ports. TCP/IP ports are pre-assigned
by the master. It starts at a specified port number and then increments
for each port needed on the given node.

The caretaker writes this configuration to a file and then fork/execs
the block's process, passing it the path to the configuration file and
the path to the poll data file as command line arguments. The block
reads this configuration, stores it in its object, and then starts
listening on its input ports. It has a special input port for direct
communications from the master.

After forking off the block process, the caretaker returns ``True`` to
indicate sucess and ``False`` otherwise.

The master then connects to the block's master port [#]_, sends
a "sync" message, and waits for an (empty) response. If no response is
received by the timeout, an error is signaled, and the entire startup processes
aborted.

Once all the blocks have been started successfully, the master enters its
main execution loop.

.. [#] The caretaker process is started at the beginning of the run
       by the Engage infrastructure.
       
.. [#] Note that this is a ZeroMQ connect, not a TCP/IP connect. ZeroMQ
       connects are lazy and sends may not be immediate. In this case,
       the send will not occur until the block has actually started
       listening on its receive socket. See the ZeroMQ documentation for
       details.

Polling Messages
----------------
During the run of a job, the master polls each of the caretaker nodes
at a set interval (once every 30 seconds by default). Here is an example
of the message exchange:

.. image:: poll_all_nodes.svg

The master communicates with each caretaker in turn. It sends a ``POLL``
message. This message has a single parameter: ``get_stats``, a boolean.
If ``True``, the caretaker should include system statistics.
This is requested once every 5 requests (so, 2 1/2 minutes by default).

The caretaker calls ``collect_poll_data()`` (see below) to gather the
data. It then responds to the master with a tuple consisting of the hostname,
load data from all of the workers on the node,
node-wide system statistics (CPU and memory usage, if requested),
and any fatal errors (including the full error message
and stack trace).

The load data is managed by the class ``LoadBasedResourceManager``. It uses
an instance of ``BlockPerfStates`` for each block to track requests made
to the block, requests served by this block, and total processing time.
These are used to compute load percentage and queue size.

collect_poll_data
-----------------
The message sequence chart below shows how the collection of load and
liveness data works at the worker nodes:

.. image:: liveness_check.svg

Each block process periodically writes its status data to a file specified
by the caretaker. When the caretaker receives a ``POLL`` request from the
master, it cycles through all the blocks on the node. For each block,
it reads the status data file and updates its information. It also
executes a ``ps -p`` command on the block's process to verify that it
is still alive. If it is dead, it marks the status as dead and also
looks for an error dump file (which is included in the response, if present).


Stopping Due to an Error
------------------------
If there were any fatal errors or block timeouts returned from the caretakers,
the stop procedure is initiatiated. A ``STOP ALL`` message is sent in turn to
each caretaker. Here is an example message exchange with one of the
caretaker processes:

.. image:: stop_all.svg

Upon receiving the ``STOP ALL`` message, it sends a ``SIGTERM`` signal to each
block process. It then monitors the processes to see whether they have
exited. If they have not exited within a certain number of polls, another
``SIGTERM`` is sent (e.g. block ``b2`` in our example). Finally, the block
process ``b2`` is killed with a ``SIGKILL`` signal. The caretaker process then
responds to the master with a ``True`` value message and then exits.

Job Completion
--------------
After gathering the liveness data from a poll of the caretakers, the master
checks to see whether any blocks are still running. If no blocks are running,
and we haven't seen an error, then they are all in the ``STOPPED`` state (see
``BlockStatus`` in block.py. The master then sends a ``END RUN`` message to each
of the caretakers. The caretaker stops the blocks (which should have no effect),
responds to the master with ``True``, and then exits.

After communicating with all the caretakers, the master itself prints
final summary statistics and exits.



