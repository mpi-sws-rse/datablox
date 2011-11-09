from element import *
from shard import *

import time


class Null_shard(Shard):
  name = "Null-Shard"
  
  def on_load(self, config):
    self.name = "Null-Shard"
    self.nodes = 2
    self.max_nodes = 2
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    print "NULL shard loaded"

  def minimum_nodes(self):
    return self.nodes
  
  def node_name(self):
    return "Null"
  
  def config_for_node(self, node_num):
    return {}
        
  def recv_push(self, port, log):
    print "%s sending to port %d" % (self.name, self.current_node)
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
  
  def can_add_node(self):
    return (self.nodes < self.max_nodes)
  
  def should_add_node(self, node_num):
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num