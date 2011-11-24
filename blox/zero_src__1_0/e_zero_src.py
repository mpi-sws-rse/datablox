from element import *
import time

class zero_src(Element):
  def do_task(self):
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    numzeros = self.config["num_zeros"] if self.config.has_key("num_zeros") else 10
    for i in range(0,numzeros):
      log = Log()
      log.log["value"] = [0]
      print "Sending a zero"
      self.push("output", log)
      time.sleep(sleeptime)
      yield

  def on_load(self, config):
    self.name = "0-Src"
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    print "0-Src element loaded"