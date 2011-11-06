from element import *
import time


class Null(Element):
  name = "Null"
  
  def on_load(self, config):
    self.name = "Null"
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    print "NULL element loaded"

  def recv_push(self, port, log):
    print "log is: " + str(log.log)
    #time.sleep(1)