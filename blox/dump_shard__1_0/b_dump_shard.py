import sys
import os.path
from logging import ERROR, WARN, INFO, DEBUG
import time

try:
  import datablox_framework
except ImportError:
  sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               "../../datablox_framework")))
  import datablox_framework

from datablox_framework.block import *
from datablox_framework.shard import *

class dump_shard(Shard):
  @classmethod
  def initial_configs(cls, config):
    return [config for i in range(config["nodes"])]
  
  @classmethod
  def node_type(cls):
    return {"name": "Dump", "input_port": "input", "port_type": "PUSH"}
  
  def on_load(self, config):
    self.nodes = config["nodes"]
    self.config = config
    self.max_nodes = 20
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.log(INFO, "Dump shard loaded")
  
  def config_for_new_node(self):
    return self.config
        
  def recv_push(self, port, log):
    self.log(INFO, "%s sending to port %d" % (self.id, self.current_node))
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
  
  def can_add_node(self):
    return (self.nodes < self.max_nodes)
  
  def should_add_node(self, node_num):
    self.log(INFO,self.id + " should_add_node got a new port!")
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num
