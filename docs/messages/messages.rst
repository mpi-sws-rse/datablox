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

