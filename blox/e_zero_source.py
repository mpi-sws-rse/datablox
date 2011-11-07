from element import *
import time

class ZeroSrc(Element):
  name = "0-Src"
  
  def src_start(self):
    for i in range(0,5):
      log = Log()
      log.log["value"] = [0]
      print "Sending a zero"
      self.push("output", log)
      time.sleep(1)
    self.shutdown()

  def on_load(self, config):
    self.name = "0-Src"
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    print "0-Src element loaded"