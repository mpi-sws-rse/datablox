from block import *
from logging import ERROR, WARN, INFO, DEBUG

class simple_query(Block):
  def on_load(self, config):
    self.name = "Simple-query"
    self.add_port("input", Port.QUERY, Port.UNNAMED, ["number"])
    self.log(INFO, "Simple-query block loaded")

  def recv_query(self, port, log):
    # self.log(INFO, "got query " + str(log.log))
    number = log.log["number"]
    self.log(INFO, self.name + " got query for number " + str(number))
    ret_log = Log()
    ret_log.log["result"] = number + 1
    self.return_query_res(port, ret_log)
