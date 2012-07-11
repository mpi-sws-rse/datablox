"""This is a python representation of the topology. Currently not used by the
master itself, but instead used by external utilities. Eventually should be
used by the GroupHandler class.

TODO: Add support for block groups.
"""

import json

class ParseError(Exception):
    pass

class Block(object):
    def __init__(self, block_id, name, args=None, at=None,
                 is_shard_element=False):
        self.block_id = block_id
        self.name = name
        if args:
            self.args = args
        else:
            self.args = {}
        self.input_ports = {} # from port name to source block
        self.output_ports = {} # from port name to list of dest blocks
        self.at = at
        self.is_shard_element = is_shard_element

    def is_shard(self):
        """This is just a heuristic, since there's know way to really know
        from the syntax.
        """
        return self.args.has_key("nodes") and self.args.has_key("node_type")

    def get_shard_details(self):
        """Given this is a shard, return a tuple consisting of
        the number of shard elements and the "at" value for each element.
        If an element has not value for "at", then None is used.
        """
        assert self.is_shard()
        nodes = self.args["nodes"]
        assert isinstance(nodes, int)
        if self.args["node_type"].has_key("args"):
            return (nodes,
                    [d["at"] if d.has_key("at") else None for d in self.args["node_type"]["args"]])
        else:
            return (nodes, [None for i in range(len(nodes))])
        
    def add_input(self, name, source_block):
        if self.input_ports.has_key(name):
            raise ParseError("Block %s input port %s has multiple sources" %
                             (self.block_id, name))
        self.input_ports[name] = source_block

    def add_output(self, name, dest_block):
        if not self.output_ports.has_key(name):
            self.output_ports[name] = []
        (self.output_ports[name]).append(dest_block)


class Topology(object):
    def __init__(self, blocks, blocks_by_id, connections, nodes, unmapped_blocks):
        self.blocks = blocks
        self.blocks_by_id = blocks_by_id
        self.connections = connections
        self.nodes = nodes
        self.unmapped_blocks = unmapped_blocks

    def expand_shards(self):
        """Treat the elements of a shard as regular blocks and add them to the
        topology.
        """
        for b in self.blocks:
            if not b.is_shard():
                continue
            (num_elements, ats) = b.get_shard_details()
            for e_num in range(num_elements):
                e_id = b.block_id + ("-%d" % e_num)
                sb = Block(e_id,
                           b.args["node_type"]["name"],
                           args=b.args["node_type"]["args"][e_num],
                           at=ats[e_num],
                           is_shard_element=True)
                self.blocks.append(sb)
                self.blocks_by_id[e_id] = sb
                if sb.at:
                    if not self.nodes.has_key(sb.at):
                        self.nodes[sb.at] = []
                    (self.nodes[sb.at]).append(sb)
                else:
                    self.unmapped_blocks.append(sb)
                ip = b.args["node_type"]["input_port"] \
                     if b.args["node_type"].has_key("input_port") \
                     else "input"
                sb.add_input(ip, b)
                op = "%d" % e_num
                b.add_output(op, sb)
                self.connections.append((b, op, sb, ip),)
                    
            
    @staticmethod
    def create(topology_file):
        with open(topology_file, "rb") as f:
           data = json.load(f)
        if not data.has_key("blocks"):
            raise ParseError("Topology file %s missing 'blocks' property" %
                             topology_file)
        block_json = data["blocks"]
        blocks = []
        blocks_by_id = {}
        nodes = {}
        unmapped_blocks = []
        for i in range(len(block_json)):
            bj = block_json[i]
            for key in ["id", "name"]:
                if not bj.has_key(key):
                    raise ParseError("Block %d in topology file %s missing '%s' property" %
                                     (i, key))
            if bj.has_key("at"):
                at = bj["at"]
            else:
                at = None
            b = Block(bj["id"], bj["name"],
                      bj["args"] if bj.has_key("args") else None, at)
            blocks.append(b)
            blocks_by_id[b.block_id] = b
            if at:
                if not nodes.has_key(at):
                    nodes[at] = []
                (nodes[at]).append(b)
            else:
                unmapped_blocks.append(b)
        if not data.has_key("connections"):
            raise ParseError("Topology file %s missing 'connections' property" %
                             topology_file)
        connections = []
        for i in range(len(data["connections"])):
            conn = data["connections"][i]
            if len(conn)!=2:
                raise ParseError("Connection %d has %d elements, should have 2" % (i, len(conn)))
            src_id = conn[0].keys()[0]
            dest_id = conn[1].keys()[0]
            if blocks_by_id.has_key(src_id):
                src = blocks_by_id[src_id]
            else:
                raise ParseError("Connection %d refers to non-existant source block %s" %
                                 (i, src_id))
            if blocks_by_id.has_key(dest_id):
                dest = blocks_by_id[dest_id]
            else:
                raise ParseError("Connection %d refers to non-existant dest block %s" %
                                 (i, dest_id))
            src_port = conn[0][src_id]
            dest_port = conn[1][dest_id]
            src.add_output(src_port, dest)
            dest.add_input(dest_port, src)
            connections.append((src, src_port, dest, dest_port),)
        return Topology(blocks, blocks_by_id, connections, nodes, unmapped_blocks)
        
