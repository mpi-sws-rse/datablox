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


class queue_shard(Shard):
  @classmethod
  def initial_configs(cls, config):
    if isinstance(config["node_type"]["args"], list):
      #at least have as many arguments as there are nodes
      assert(len(config["node_type"]["args"]) >= config["nodes"])
      return [config["node_type"]["args"][i] for i in range(config["nodes"])]
    else:
      return [config["node_type"]["args"] for i in range(config["nodes"])]
  
  def on_load(self, config):
    self.nodes = config["nodes"]
    self.config = config
    self.pending_reqs = []
    self.pending_workers = range(self.nodes)
    self.requests_served = 0
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("input_query", Port.QUERY, Port.UNNAMED, [])
    self.log(INFO, "Queue-Shard loaded")
  
  def config_for_new_node(self):
    return self.config["nodes"].args
  
  #will try to distribute work if possible
  def distribute(self):
    #give each worker one request
    while(self.pending_reqs != [] and self.pending_workers != []):
      req = self.pending_reqs[0]
      worker = self.pending_workers[0]
      self.pending_reqs = self.pending_reqs[1:]
      self.pending_workers = self.pending_workers[1:]
      self.log(INFO, "Sending to port %d" % worker)
      self.query_node(worker, req, async=True, add_time=True)
      self.requests_served += 1
      self.find_port("input_query").requests = self.requests_served
  
  def new_req(self, log):
    self.pending_reqs.append(log)
    self.distribute()
  
  def new_worker(self, num):
    self.pending_workers.append(num)
    self.distribute()
    
  def recv_push(self, port, log):
    self.new_req(log)
    self.find_port("input").requests = self.requests_served - 1
  
  def recv_query_result_num(self, node_num, log):
    self.log(INFO, "Got result from %d: %r" % (node_num, str(log)))
    self.new_worker(node_num)
    
  def can_add_node(self):
    return True
  
  def should_add_node(self, node_num):
    self.log(INFO,self.id + " should_add_node got a new port!")
    self.nodes += 1
    # start distribution from the new node
    self.current_node = node_num

  def recv_query(self, port, log):
    self.new_req(log)
    self.find_port("input_query").requests = self.requests_served - 1
    ret = Log()
    ret.log["result"] = True
    self.return_query_res(port, ret)