from block import *
import base64
from logging import ERROR, WARN, INFO, DEBUG

class restore_manager(Block):
  def on_load(self, config):
    self.name = "Restore-Manager"
    self.config = config
    self.filepath = config["restore_file"]
    self.add_port("chunk_index", Port.QUERY, Port.UNNAMED, ["fingerprint"])
    self.add_port("meta_index", Port.QUERY, Port.UNNAMED, ["path"])
  
  def fetch_file_fps(self, path):
    log = Log()
    log.append_field("path", [path])
    fps_log = self.query("meta_index", log)
    assert(len(fps_log.log["fingerprints"]) == 1)
    return fps_log.log["fingerprints"][0]
  
  def fetch_chunks(self, fps):
    log = Log()
    log.append_field("fingerprint", fps)
    clog = self.query("chunk_index", log)
    return clog.log["chunk"]
  
  def concat_chunks(self, chunks):
    return base64.b64decode(''.join(chunks))
    #return ''.join(chunks)
  
  def do_task(self):
    fps = self.fetch_file_fps(self.filepath)
    self.log(INFO, fps)
    chunks = self.fetch_chunks(fps)
    self.log(INFO, chunks)
    with open('restored', 'w') as f:
      f.write(self.concat_chunks(chunks))
      
    #print > "restored" self.concat_chunks(chunks)
    yield