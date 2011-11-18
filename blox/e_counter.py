from element import *
import time

class counter(Element):
  def src_start(self):
    for i in range(5):
      log = Log()
      log.log["value"] = [self.count]
      print "Sending " + str(self.count)
      self.push("output", log)
      self.count = self.count + 1
      time.sleep(1)
    self.shutdown()

  def on_load(self, config):
    self.name = "Counter"
    self.count = 0
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    print "Counter-Src element loaded"