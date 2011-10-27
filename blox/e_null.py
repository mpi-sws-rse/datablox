from element import *

class Null(Element):
  name = "Null"
  
  def on_load(self, config):
    self.name = "Null"
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    print "NULL element loaded"

  def recv_push(self, port, log):
    print "dumping log from " + port.end_point.name
    print "log is: " + str(log.log["value"])