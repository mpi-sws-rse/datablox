"""This is a shard that works off a fixed set of values. The shard_field
configuration property should be set to an incoming message property that
can be used to select a shard. Each node's definition should have a property
called shard_field_value. This is used to build a mapping from vlaues of
the shard_field to nodes.
"""
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


class EnumShardError(Exception):
  pass

class ValueNotInEnum(Exception):
  def __init__(self, v):
    Exception.__init__(self, "Value '%s' not found in Enum" % v)
    self.v = v
    
class enum_shard(Shard):
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
    self.shard_field = config["shard_field"]
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("input_query", Port.QUERY, Port.UNNAMED, [])
    self.field_to_node_mapping = {}
    for i in range(config["nodes"]):
      node_info = config["node_type"]["args"][i]
      if not node_info.has_key("shard_field_value"):
        raise EnumShardError("Shard %d missing shard_field_value property" % i)
      v = node_info["shard_field_value"]
      if self.field_to_node_mapping.has_key(v):
        raise EnumShardError("Shard has multiple nodes defined for field value %s" %
                             v)
      self.field_to_node_mapping[v] = i
    self.log(INFO, "field to node mapping: %r" % self.field_to_node_mapping)
    self.log(INFO, "Enum shard loaded")
  
  def find_node_num(self, row):
    val = row[self.shard_field]
    if self.field_to_node_mapping.has_key(val):
      return self.field_to_node_mapping[val]
    else:
      raise ValueNotInEnum(val)
  
  def flush_logs(self, logs):
    for p_num, log in logs.items():
      self.push_node(p_num, log)
    
  def process_log(self, log):
    logs = defaultdict(Log)
    for row in log.iter_flatten():
      try:
        p = self.find_node_num(row)
        logs[p].append_row(row)
      except KeyError:
        #this row does not have shard field - send it to all ports
        #useful for sending tokens
        #first flush all the pending logs, because this doesn't have the same names
        self.flush_logs(logs)
        logs = defaultdict(Log)
        nl = Log()
        nl.append_row(row)
        for i in range(self.nodes):
          self.push_node(i, nl)
      except ValueNotInEnum, e:
        #this row's shard field value not in enum- send it to all ports
        #first flush all the pending logs, because this doesn't have the same names
        self.flush_logs(logs)
        logs = defaultdict(Log)
        self.log(WARN,"%s, sending to all ports" % e)
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
