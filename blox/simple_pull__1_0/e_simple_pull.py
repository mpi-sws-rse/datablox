from element import *
from logging import ERROR, WARN, INFO, DEBUG

class simple_pull(Element):
  def on_load(self, config):
    self.name = "Simple-pull"
    self.add_port("input", Port.PULL, Port.UNNAMED, ["number"])
    self.log(INFO, "Simple-pull element loaded")

  def recv_pull_query(self, port, log):
    # self.log(INFO, "got query " + str(log.log))
    number = log.log["number"]
    self.log(INFO, self.name + " got query for number " + str(number))
    ret_log = Log()
    ret_log.log["result"] = number + 1
    self.return_pull(port, ret_log)
