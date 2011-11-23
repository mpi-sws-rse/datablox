Datablox:

 - compositional framework for large-scale data analytics
 - data-analytics: collecting, analyzing and archiving real-time data from various sources
 - compositional: the design allows for building systems out of small, well-defined components and allows for significant reuse.
 - large-scaled: designed to work on private clusters and the cloud. Is capable of dynamically parallelizing the system based on loads.

Datablox provides the following components:

1. An API for building modules
2. A domain specific language (DSL) for composing various modules together
3. A type-checker to ensure that the connections are valid and privacy conditions are followed
4. A runtime which distributes the modules across various nodes, ensures that they communicate with each other, restarts nodes which crash, provides data integrity guarantees and dynamically parallelizes certain modules based on loads.

Related work:

There exist several frameworks which allow certain kinds of data analytics but they all lack in certain aspects:

 - Map-reduce: 
  -- Good for tasks which can be parallelized naturally into map-reduce tasks (embarrassingly parallel). Can work with large datasets and scales well. 

  -- Does not support real-time data processing as it wouldn't know how to split up tasks. Very restricted in the kinds of computations it allows - e.g most iterative/recursive algorithms cannot be implemented. Most map-reduce implementations only distribute code and not libraries and other dependencies on the code. Not easy for non-programmers to build systems out of components as it usually requires some programming.

  -- Does not have primitives for data queries and archival using databases. Needs a homogenous distributed file system or database available to all modules

 - Dryand, Hadoop: 
  -- Similar to map-reduce but have to check
  
 - Flume/Storm:
  -- Good for real-time push processing of data. Very good for tasks like trending topics, word count etc.

  -- Does not support queries from the modules. For example a task to measure influence of a user, which can be defined by the number of people RT-him does not fit into that design

 - Cloud scripts like Skywriting:   -- Allows for more classes of computation than simple map-reduce

  -- Have most of the disadvantages of map-reduce. Only work with code in certain languages, have no way to specify software dependencies

- Ad-hoc implementations:
  -- Maybe efficient for the task at hand and for tested inputs

  -- Do not scale, do not provide data guarantees, do not provide ways to retrieve from node crashes etc

Example:

A file-system analytics tool, which runs on a large number of systems in an organization. This tool involves the following components: 

 - File-system crawler: which extracts details about file systems across various nodes

 - Categorizer: categorizes files based on their file types

 - Indexer: indexes the data in the files for easy queries

 - Metadata-indexer: indexes the metadata in the files

 - Query-system: which talks to the indexer and metadata-indexer and presents reports on the status of the systems

This has several use-cases: system administrators might want to know how many duplicates of files exist in the network, or make sure certain `unsafe' files (mp3, exe) are not present in user's hard disks etc.

Design:

See Design.rst

Implementation

See Design.rst

Experiments

See the examples