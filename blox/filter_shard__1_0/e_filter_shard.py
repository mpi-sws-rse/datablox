from element import *
from shard import *

import time


class filter_shard(Shard):
  @classmethod
  def initial_configs(cls, config):
    return [config for i in range(config["nodes"])]
  
  @classmethod
  def node_type(self):
    return {"name": "Filter-words", "input_port": "input", "output_port": "output", "port_type": "PUSH"}
  
  def on_load(self, config):
    self.name = "Filter-Shard"
    self.config = config
    self.nodes = config["nodes"]
    self.max_nodes = 26
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    print "Filter shard loaded"
  
  def config_for_new_node(self):
    return self.config
        
  def recv_push(self, port, log):
    #print "%s sending to port %d" % (self.name, self.current_node)
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
  
  def can_add_node(self):
    return (self.nodes < self.max_nodes)
  
  def should_add_node(self, node_num):
    print self.name + " should_add_node got a new node"
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num