from block import *
import time
import os
import os.path
import socket
import base64
from logging import ERROR, WARN, INFO, DEBUG

# file logging will only show up if we set the log level to ALL
FILE_LOGGING=DEBUG-1

class file_crawler(Block):
  def send_file(self, host, volume, path, stat):
    listing = {}
    listing["path"] = [host + ":" + path]
    listing["size"] = [stat.st_size]
    listing["perm"] = [stat.st_mode]
    listing["owner"] = [stat.st_uid]
    listing["volume"] = [volume]
    listing["url"] = [BlockUtils.generate_url_for_path(path)]
    
    log = Log()
    log.set_log(listing)
    self.buffered_push("output", log)
  
  def send_token(self, volume_name):
    token = {"token": volume_name}
    log = Log()
    log.set_log(token)
    self.buffered_push("output", log)
    self.flush_port("output")
    
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
    self.log(DEBUG, "Starting walk at %s" % path)
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        fpath = os.path.join(root, filename)
        try:
          stat = os.stat(fpath)
          self.log(FILE_LOGGING, "Sending fie %s" % fpath)
          self.send_file(host, volume_name, fpath, stat)
          files_sent += 1
          if files_sent > self.single_session_limit:
            files_sent = 0
            yield
        except OSError:
          self.log(WARN, "not dealing with file " + fpath)
          continue
    yield
    #this will clear all outstanding files in the buffer
    self.send_token(volume_name)
    
  def on_load(self, config):
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner" ,"volume", "url"])
    self.single_session_limit = 50
    self.log(INFO, "File-Crawler block loaded")
