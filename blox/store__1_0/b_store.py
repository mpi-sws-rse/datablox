from block import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG

class store(Block):
  def on_load(self, config):
    self.config = config
    self.store_dir = os.path.expanduser(self.config["store_directory"])
    self.add_port("input", Port.QUERY, Port.UNNAMED, ["chunk"])
    self.add_port("control", Port.QUERY, Port.UNNAMED, ["command", "args"])

  def do_store(self, chunk_url):
    #str doesn't give enough digits
    name = time.time().__repr__()
    path = os.path.join(self.store_dir, name)
    chunk = BlockUtils.fetch_file_at_url(chunk_url)
    with open(path, 'w') as f:
      f.write(chunk)
    return name
  
  def do_restore(self, chunk_id):
    path = os.path.join(self.store_dir, chunk_id)
    if os.path.exists(path):
      return BlockUtils.generate_url_for_path(path)
    else:
      self.log(WARN, "could not find chunk with id: %r" % chunk_id)
      return ''

  def delete(self, chunk_id):
    path = os.path.join(self.store_dir, chunk_id)
    try:
      os.unlink(path)
      return True
    except Exception as e:
      self.log(WARN, "could not delete chunk with id: %r, due to %r" % (chunk_id, e))
      return False
    
  def recv_query(self, port, log):
    retlog = Log()
    if port == "input":
      chunks = log.log["chunk"]
      chunk_ids = [self.do_store(c) for c in chunks]
      retlog.append_field("result", chunk_ids)
    elif port == "control":
      assert(len(log.log["command"]) == 1)
      assert(len(log.log["args"]) == 1)
      command, args = log.log["command"][0], log.log["args"][0]
      
      if command == "restore":
        chunk_ids = args
        chunk_urls = [self.do_restore(i) for i in chunk_ids]
        retlog.append_field("chunk", chunk_urls)
      elif command == "delete":
        chunk_ids = args
        results = [self.delete(i) for i in chunk_ids]
        retlog.append_field("result", results)
        
    self.return_query_res(port, retlog)
    