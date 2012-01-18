from element import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG

class chunk(Element):
  def on_load(self, config):
    #self.buffer_limit = 5000
    self.name = "Chunk"
    self.config = config
    #default to 4KB chunks if size is not given, but I'm not sure if python 'char' is one byte
    self.chunk_size = config.get("chunk_size", 4096)
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "data"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "chunks"])
  
  def chunk(self, data):
    return [data[i:i+self.chunk_size] for i in range(0, len(data), self.chunk_size)]
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.name + " got the finish token for directory " + log.log["token"])
    else:
      chunks = []
      for d in log.log["data"]:
        chunks.append(self.chunk(d))
    
      new_log = Log()
      new_log.append_field("path", log.log["path"])
      new_log.append_field("chunks", chunks)
      self.buffered_push("output", new_log)