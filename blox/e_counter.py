from element import *
import time

class ZeroSrc(Element):
  name = "Counter-Src"
  
  def src_start(self):
    while(True):
      log = Log()
      log.log["value"] = [self.count]
      print "Sending " + str(self.count)
      self.push("output", log)
      self.count = self.count + 1
      time.sleep(1)

  def on_load(self, config):
    self.name = "Counter-Src"
    self.count = 0
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    print "Counter-Src element loaded"