from element import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG

class categorize(Element):
  def on_load(self, config):
    self.name = "Categorize"
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "name", "size", "perm", "owner", "category"])

  def find_category(self, file_name):
    if file_name.endswith(".py"):
      return "Source"
    elif file_name.endswith(".txt"):
      return "Text"
    elif file_name.endswith(".mp3"):
      return "Media"
    elif file_name.endswith(".pyc"):
      return "Compiled Source"
    else:
      return "Unknown"
  
  def get_categories(self, log):
    paths = log["path"]
    categories = [self.find_category(p) for p in paths]
    return categories
  
  def get_names(self, log):
    paths = log["path"]
    names = [os.path.split(p)[-1] for p in paths]
    return names
    
  def comment(self, path):
    p = os.popen("file " + path)
    return p.read()
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.name + " got the finish token for directory " + log.log["token"])
    else:
      log.append_field("name", self.get_names(log.log))
      log.append_field("category", self.get_categories(log.log))
          
    self.buffered_push("output", log)
    if self.config.has_key("sleep"):
      time.sleep(self.config["sleep"])