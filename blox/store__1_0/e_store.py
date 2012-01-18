from element import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG

class store(Element):
  def on_load(self, config):
    self.name = "store"
    self.config = config
    self.store_dir = os.path.expanduser(self.config["store_directory"])
    self.add_port("input", Port.PULL, Port.UNNAMED, ["chunk"])
    self.add_port("restore", Port.PULL, Port.UNNAMED, ["chunk_id"])
    self.add_port("control", Port.PULL, Port.UNNAMED, ["command", "args"])

  def do_store(self, chunk):
    #str doesn't give enough digits
    name = time.time().__repr__()
    path = os.path.join(self.store_dir, name)
    #may want to decode from base64
    with open(path, 'w') as f:
      f.write(chunk)
    return name
  
  def do_restore(self, chunk_id):
    path = os.path.join(self.store_dir, chunk_id)
    with open(path, 'r') as f:
      return f.read()
    
  def recv_pull_query(self, port, log):
    if port == "input":
      chunks = log.log["chunk"]
      chunk_ids = [self.do_store(c) for c in chunks]
      ret_log = Log()
      ret_log.append_field("result", chunk_ids)
      self.return_pull(port, ret_log)
    elif port == "restore":
      chunk_ids = log.log["chunk_id"]
      chunks = [self.do_restore(i) for i in chunk_ids]
      log.append_field("chunk", chunks)
      self.return_pull(port, log)
    else:
      self.log(INFO, "**%s did not implement actions on port %s" % (self.name, port))