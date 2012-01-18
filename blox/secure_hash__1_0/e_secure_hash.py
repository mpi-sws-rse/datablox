from element import *
import os
import hashlib
from logging import ERROR, WARN, INFO, DEBUG

class secure_hash(Element):
  def on_load(self, config):
    self.name = "Secure-Hash"
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["data"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["data", "hash"])
    self.add_port("query", Port.PULL, Port.UNNAMED, ["data"])
  
  def hash(self, c):
    return hashlib.sha224(c).hexdigest()

  def get_hashes(self, log):
    try:
      data_list = log["data"]
      hash_list = [self.hash(d) for d in data_list]
      return hash_list
    except KeyError:
      self.log(ERROR, "No data in log: %r" % log)
      return []
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.name + " got the finish token for directory " + log.log["token"])
    else:
      hashes = self.get_hashes(log.log)
      log.append_field("hash", hashes)

    self.push("output", log)
  
  def recv_pull_query(self, port_name, log):
    nl = Log()
    hashes = self.get_hashes(log)
    nl.set_log({"hash": hashes})
    self.return_pull(port_name, nl)