from element import *
import time

class Count(Element):
  name = "Count"
  
  def on_load(self, config):
    self.name = "Count"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["value"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    self.count = 0
    print "NULL element loaded"

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
