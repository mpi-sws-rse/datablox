from element import *
import pymongo
from pymongo import Connection

class Categorize(Element):
  name = "Categorize"
  
  def on_load(self, config):
    self.name = "Categorize"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner", "category"])

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
    
  def recv_push(self, port, log):
    new_log = {}
    if log.log.has_key("token"):
      print self.name + " got the finish token for directory " + log.log["token"]
      new_log = {}
      new_log["token"] = log.log["token"]
    else:
      log = log.log
      names = log["name"]
      categories = []
    
      for name in names:
        categories.append(self.find_category(name))
      
      new_log["name"] = names
      new_log["size"] = log["size"]
      new_log["perm"] = log["perm"]
      new_log["owner"] = log["owner"]
      new_log["category"] = categories
    
    nl = Log()
    nl.set_log(new_log)
    self.push("output", nl)