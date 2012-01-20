from block import *
import time
from logging import ERROR, WARN, INFO, DEBUG

class zero_src(Block):
  def do_task(self):
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    numzeros = self.config["num_zeros"] if self.config.has_key("num_zeros") else 10
    for i in range(0,numzeros):
      log = Log()
      log.log["value"] = [0]
      self.log(INFO, "Sending a zero")
      self.push("output", log)
      time.sleep(sleeptime)
      yield

  def on_load(self, config):
    self.name = "0-Src"
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    self.log(INFO, "0-Src block loaded")