import sys
import os.path
from logging import ERROR, WARN, INFO, DEBUG
import time
from collections import defaultdict

try:
  import datablox_framework
except ImportError:
  sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               "../../datablox_framework")))
  import datablox_framework

from datablox_framework.block import *
from datablox_framework.shard import *


class hash_shard(Shard):
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
    self.hash_field = config["hash_field"]
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("input_query", Port.QUERY, Port.UNNAMED, [])
    self.log(INFO, "Hash shard loaded")
  
  #python's hash function is not deterministic, 
  #but will most likely give the same hash value for same input values
  #this may need to change in the future
  def find_node_num(self, row):
    val = row[self.hash_field]
    return hash(val) % self.nodes
  
  def flush_logs(self, logs):
    for p_num, log in logs.items():
      self.push_node(p_num, log)
    
  def process_log(self, log):
    logs = defaultdict(Log)
    for row in log.iter_flatten():
      try:
        p = self.find_node_num(row)
        # self.log(INFO, "sending to port %d" % p)
        logs[p].append_row(row)
      #this row does not have the unique hash value, send it to all ports
      #useful for sending tokens
      except KeyError:
        #first flush all the pending logs, because this doesn't have the same names
        self.flush_logs(logs)
        logs = defaultdict(Log)
        # self.log(INFO, "sending to all ports")
        nl = Log()
        nl.append_row(row)
        for i in range(self.nodes):
          self.push_node(i, nl)
    self.flush_logs(logs)
      
  def recv_push(self, port, log):
    self.process_log(log)
      
  #migration not implemented yet
  def can_add_node(self):
    return False
  
  def recv_query(self, port, log):
    self.process_log(log)
    ret = Log()
    ret.log["result"] = True
    self.return_query_res(port, ret)
