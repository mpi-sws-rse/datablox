from element import *
import time


class Dump(Element):
  name = "Dump"
  
  def on_load(self, config):
    self.name = "Dump"
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.sleep_time = config["sleep"] if config.has_key("sleep") else 1
    print "Dump element loaded"

  def recv_push(self, port, log):
    print "log is: " + str(log.log)
    time.sleep(self.sleep_time)  
