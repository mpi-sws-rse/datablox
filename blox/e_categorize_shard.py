from element import *
from shard import *

import time


class Categorize_shard(Shard):
  name = "Categorize-Shard"
  
  def on_load(self, config):
    self.name = "Categorize-Shard"
    self.config = config
    self.nodes = config["nodes"]
    self.max_nodes = 20
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    print "Categorize shard loaded"

  def minimum_nodes(self):
    return self.nodes
  
  def node_type(self):
    return {"name": "Categorize", "input_port": "input", "output_port": "output", "port_type": "PUSH"}
  
  def config_for_new_node(self):
    return self.config
        
  def recv_push(self, port, log):
    print "%s sending to port %d" % (self.name, self.current_node)
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
  
  def can_add_node(self):
    return (self.nodes < self.max_nodes)
  
  def should_add_node(self, node_num):
    print self.name + " should_add_node got a new node"
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num