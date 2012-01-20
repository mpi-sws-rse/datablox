from block import *
from shard import *
from logging import ERROR, WARN, INFO, DEBUG
import time


class null_shard(Shard):
  def on_load(self, config):
    self.nodes = config["nodes"]
    self.config = config
    self.max_nodes = 20
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.log(INFO, "NULL shard loaded")

  def minimum_nodes(self):
    return self.nodes
  
  def node_type(self):
    return {"name": "Null", "input_port": "input", "port_type": "PUSH"}
  
  def config_for_new_node(self):
    return self.config
        
  def recv_push(self, port, log):
    # self.log(INFO, "%s sending to port %d" % (self.id, self.current_node))
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
  
  def can_add_node(self):
    return (self.nodes < self.max_nodes)
  
  def should_add_node(self, node_num):
    self.log(INFO, self.id + " should_add_node got a new port!")
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num