from element import *
import time
import os

class DirSrc(Element):
  name = "Dir-Src"
  
  def src_start(self):
    path = self.config["directory"]
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        path = os.path.join(root, filename)
        if path.find('/index/') != -1 or path.find('.git/') != -1:
          continue
        stat = os.stat(path)
        listing = {}
        listing["path"] = [path]
        listing["size"] = [stat.st_size]
        listing["perm"] = [stat.st_mode]
        listing["owner"] = [stat.st_uid]
        log = Log()
        log.set_log(listing)
        self.push("output", log)
        time.sleep(sleeptime)
    
    token = {"token": self.config["directory"]}
    log = Log()
    log.set_log(token)
    self.push("output", log)
    
    self.shutdown()

  def on_load(self, config):
    self.config = config
    self.name = "Dir-Src:" + config["directory"]
    # self.name = "Dir-Src"
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    print "Dir-Src element loaded"