import os
import sys
from block import *
from logging import ERROR, WARN, INFO, DEBUG

class pick_mp3(Block):
  def on_load(self, config):
    self.name = "pick-mp3"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name"])
  
  def recv_push(self, port, log):
    files = log.log["name"]
    for path in files:
      if path.endswith(".mp3"):
        self.log(INFO, "FOUND an mp3: " + path)
  
