from block import *
import time
from logging import ERROR, WARN, INFO, DEBUG

class counter(Block):
  def do_task(self):
    for i in range(5):
      log = Log()
      log.log["value"] = [self.count]
      self.log(INFO, "Sending " + str(self.count))
      self.push("output", log)
      self.count = self.count + 1
      time.sleep(1)
      yield

  def on_load(self, config):
    self.name = "Counter"
    self.count = 0
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    self.log(INFO, "Counter-Src block loaded")