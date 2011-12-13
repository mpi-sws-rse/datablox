from element import *

class join(Element):
  def on_load(self, config):
    self.name = "Join"
    for i in range(config["joins"]):
      self.add_port("input"+str(i+1), Port.PUSH, Port.UNNAMED, [])

    self.add_port("output", Port.PUSH, Port.UNNAMED, [])

  def recv_push(self, port, log):
    nl = Log()
    nl.set_log(log.log)
    self.buffered_push("output", nl)