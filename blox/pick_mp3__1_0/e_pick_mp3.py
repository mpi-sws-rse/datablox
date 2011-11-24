import os
import sys
from element import *

class pick_mp3(Element):
  def on_load(self, config):
    self.name = "pick-mp3"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name"])
  
  def recv_push(self, port, log):
    files = log.log["name"]
    for path in files:
      if path.endswith(".mp3"):
        print "FOUND an mp3: " + path
  
