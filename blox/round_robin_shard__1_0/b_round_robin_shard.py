from block import *
from shard import *
from logging import ERROR, WARN, INFO, DEBUG
import time


class round_robin_shard(Shard):
  @classmethod
  def initial_configs(cls, config):
    return [config["node_type"]["args"] for i in range(config["nodes"])]
  
  def on_load(self, config):
    self.nodes = config["nodes"]
    self.config = config
    self.max_nodes = 20
    self.current_node = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("input_query", Port.QUERY, Port.UNNAMED, [])
    self.log(INFO, "Round-Robin shard loaded")
  
  def config_for_new_node(self):
    return self.config["nodes"].args
        
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

  def recv_query(self, port, log):
    self.log(INFO, "%s sending to port %d" % (self.id, self.current_node))
    self.push_node(self.current_node, log)
    self.current_node = (self.current_node + 1) % self.nodes
    ret = Log()
    ret.log["result"] = True
    self.return_query_res(port, ret)