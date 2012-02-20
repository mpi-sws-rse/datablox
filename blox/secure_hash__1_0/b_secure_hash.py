from block import *
import os
import hashlib
from logging import ERROR, WARN, INFO, DEBUG

class secure_hash(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["url"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["url", "fingerprint"])
    self.add_port("query", Port.QUERY, Port.UNNAMED, ["url"])
  
  def hash(self, c):
    return hashlib.sha224(c).hexdigest()

  def get_hashes(self, log):
    data_urls = log["url"]
    hash_list = [self.hash(BlockUtils.fetch_file_at_url(u)) for u in data_urls]
    return hash_list
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.id + " got the finish token for directory " + log.log["token"])
    else:
      hashes = self.get_hashes(log.log)
      log.append_field("fingerprint", hashes)

    self.push("output", log)
  
  def recv_query(self, port_name, log):
    nl = Log()
    hashes = self.get_hashes(log)
    nl.set_log({"fingerprint": hashes})
    self.return_query_res(port_name, nl)