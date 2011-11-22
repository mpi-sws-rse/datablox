from element import *
import time
import os
import socket

class dir_src(Element):
  def indexible_file(self, path):
    return path.endswith('.txt') or path.endswith('.py') or path.endswith('.java')
  
  def do_task(self):
    path = os.path.expanduser(self.config["directory"])
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    #using the ip-address for now
    try:
      volume_name = socket.gethostbyname(socket.gethostname())
    except:
      volume_name = "local"
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        path = os.path.join(root, filename)
        if path.find('/index/') != -1 or path.find('.git/') != -1 or (not self.indexible_file(path)):
          continue
        try:
          stat = os.stat(path)
        except:
          print "not dealing with file " + path
          continue
        
        listing = {}
        listing["path"] = [volume_name + ":" + path]
        listing["size"] = [stat.st_size]
        listing["perm"] = [stat.st_mode]
        listing["owner"] = [stat.st_uid]
        # with open(path) as f:
        #   listing["data"] = [f.read()]
        
        log = Log()
        log.set_log(listing)
        self.push("output", log)
        time.sleep(sleeptime)
        yield
    
    token = {"token": self.config["directory"]}
    log = Log()
    log.set_log(token)
    self.push("output", log)
    
    self.shutdown()

  def on_load(self, config):
    self.config = config
    self.name = "Dir-Src:" + config["directory"]
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    print "Dir-Src element loaded"