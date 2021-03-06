from block import *
import time
from logging import ERROR, WARN, INFO, DEBUG

class count(Block):
  def on_load(self, config):
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["value"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    self.count = 0
    self.log(INFO, "NULL block loaded")

  def recv_push(self, port, log):
    self.count = self.count + 1
    values = log.log["value"]
    def add_count(v):
      return v + self.count
    new_values = map(add_count, values)
    new_log = Log();
    new_log.log["value"] = new_values
    self.push("output", new_log)
    time.sleep(2)
