from element import *
import time


class null(Element):
  def on_load(self, config):
    self.name = "Null"
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.sleep_time = config["sleep"] if config.has_key("sleep") else 0
    self.logs = 0
    print "NULL element loaded"

  def recv_push(self, port, log):
    #print "log is: " + str(log.log)
    self.logs += 1
    time.sleep(self.sleep_time)
  
  def on_shutdown(self):
    print "got %d logs" % self.logs