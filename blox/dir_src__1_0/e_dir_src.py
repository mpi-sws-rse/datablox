from element import *
import time
import os
import os.path
import socket
import base64
from logging import ERROR, WARN, INFO, DEBUG

import filetype_utils

class dir_src(Element):
  def indexable_file(self, path):
    """If true, the file can be indexed by the indexing engine (e.g. contains
    text content). We will only send the data if this returns True.
    """
    if self.only_index:
      for e in self.only_index:
        if path.endswith(e):
          return True
      return False
    else:
      return filetype_utils.is_indexable_file(path)

  def include_file(self, path):
    """Returns True if we should send this file's metadata (and potentially
    content if it is indexable).
    """
    return True
    
  def send_file(self, host, volume, path, stat):
    listing = {}
    listing["path"] = [host + ":" + path]
    listing["size"] = [stat.st_size]
    listing["perm"] = [stat.st_mode]
    listing["owner"] = [stat.st_uid]
    (filetype,category) = filetype_utils.get_file_description_and_category(path)
    listing["volume"] = [volume,]
    listing["filetype"] = [filetype,]
    listing["category"] = [category,]
    if (not self.config.has_key('only_metadata') or self.config['only_metadata'] == False) and self.indexable_file(path):
      with open(path) as f:
        listing["data"] = [base64.b64encode(f.read())]
    
    log = Log()
    log.set_log(listing)
    self.buffered_push("output", log)
  
  def send_token(self, volume_name):
    token = {"token": volume_name}
    log = Log()
    log.set_log(token)
    self.buffered_push("output", log)
    
  def do_task(self):
    path = os.path.abspath(os.path.expanduser(self.config["directory"]))
    files_sent = 0
    try:
      ## #using the ip-address for now
      ## volume_name = socket.gethostbyname(socket.gethostname())
      host = socket.gethostname()
    except:
      host = "local"
    volume_name = host + ":" + path
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        path = os.path.join(root, filename)
        if not self.include_file(path):
          continue
        try:
          stat = os.stat(path)
          self.send_file(host, volume_name, path, stat)
          files_sent += 1
          if files_sent > self.files_limit:
            files_sent = 0
            yield
        except OSError:
          self.log(WARN, "not dealing with file " + path)
          continue
    yield
    self.send_token(volume_name)
    
  def on_load(self, config):
    self.config = config
    self.name = "Dir-Src:" + config["directory"]
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    #if only_index is not specified, we set only_index to None and then
    # use the MIME type to determine the files to index.
    self.only_index = config["only_index"] if config.has_key("only_index") else None
    self.files_limit = 50
    self.log(INFO, "Dir-Src element loaded")
