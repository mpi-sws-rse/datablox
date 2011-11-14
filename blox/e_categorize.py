from element import *
import pymongo
from pymongo import Connection
import os
import time

class Categorize(Element):
  name = "Categorize"
  
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
  
  def comment(self, path):
    p = os.popen("file " + path)
    return p.read()
    
  def recv_push(self, port, log):
    new_log = {}
    if log.log.has_key("token"):
      print self.name + " got the finish token for directory " + log.log["token"]
      new_log = {}
      new_log["token"] = log.log["token"]
    else:
      log = log.log
      paths = log["path"]
      categories = []
      names = []
      comments = []
    
      for path in paths:
        categories.append(self.find_category(path))
        comments.append(self.comment(path))
        names.append(os.path.split(path)[-1])
      
      new_log["name"] = names
      new_log["path"] = paths
      new_log["size"] = log["size"]
      new_log["perm"] = log["perm"]
      new_log["owner"] = log["owner"]
      new_log["category"] = categories
      new_log["comments"] = comments
    
    nl = Log()
    nl.set_log(new_log)
    self.push("output", nl)
    if self.config.has_key("sleep"):
      time.sleep(self.config["sleep"])