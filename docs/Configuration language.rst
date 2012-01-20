=======================
Configuration Language
=======================

The system is configured using json. Please see the examples folder for some json configuration files.

Essentially, we want the configuration file to list all the blocks that we would use, along with arguments to initialize them, and connections between blocks. 

The configuration file should provide a dictionary with two fields: "blocks" which specifies what blocks are used by the system and "connections" which specifies the connections between them.

A block object is identified by an identifier. For example, in order to utilize a directory listing block to output all the files of the current working directory, we write::

    "blocks": [
        {"id": "source", 
         "name": "Dir-Src", 
         "args": {"directory": "."}}
    ]

This creates a directory lister block with arguments {"directory": "."}. The arguments tell the block to list the specified directory. We then assign an identifier "source" to the block so we can refer to it later when we are connecting it to other blocks.

Let's add another block to it::

    "blocks": [
        {"id": "source", 
         "name": "Dir-Src", 
         "args": {"directory": "."}},
        {"id": "sink", 
        "name": "Dump", 
        "args": {}}
    ]

This creates a Dump block which just prints all the information it receives on the terminal.

Now for the connections. Let's connect the source and the sink::

  "connections": [
      [{"source": "output"}, {"sink": "input"}]
  ]

We know that "Dir-Src" has an output port called "output" from its documentation (or source code) and "Dump" block has an input port called "input". Here we specify that those ports should be connected. If we have more connections, they are listed in the same way in the "connections" list.

This gives us our configuration file::

{
    "blocks": [
        {"id": "source", 
         "name": "Dir-Src", 
         "args": {"directory": "."}},
        {"id": "sink", 
        "name": "Dump", 
        "args": {}}
    ],

    "connections": [
        [{"source": "output"}, {"sink": "input"}]
    ]
}

Save this somewhere /path/to/example.json. Now, to run the tool, cd to the datablox project directory and run::

  export BLOXPATH=`pwd`/blox
  python datablox_framework/datablox_framework/loader.py /path/to/example.json

This should print all the files of the current directory. More examples are present in the examples directory.