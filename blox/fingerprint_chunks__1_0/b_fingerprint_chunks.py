from block import *
import os
import hashlib

class fingerprint_chunks(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["chunks"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["chunks", "fingerprints"])
    #self.buffer_limit = 5000

  def hash(self, c):
    return hashlib.sha224(c).hexdigest()

  def get_hashes(self, chunks):
    hash_list = [self.hash(c) for c in chunks]
    return hash_list
    
  def recv_push(self, port, log):
    chunk_hashes = []
    for c in log.log["chunks"]:
      chunk_hashes.append(self.get_hashes(c))
    log.append_field("fingerprints", chunk_hashes)
    self.buffered_push("output", log)