from block import *

class no_transform(Block):
  def on_load(self, config):
    self.add_port("input1", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner"])
    self.add_port("input2", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner", "category"])

  def recv_push(self, port, log):
    nl = Log()
    nl.set_log(log.log)
    self.push("output", nl)