from block import *
from logging import ERROR, WARN, INFO, DEBUG
import time

class simple_query(Block):
  def on_load(self, config):
    self.add_port("input", Port.QUERY, Port.UNNAMED, ["value"])
    self.log(INFO, "Simple-query block loaded")
    self.t = 0

  def recv_query(self, port, log):
    self.log(INFO, "got query " + str(log.log))
    self.log(INFO, self.id + " got query for values " + str(log.log["value"]))
    ret_log = Log()
    ret_log.log["result"] = [number + 1 for number in log.log["value"]]
    if self.t == 0:
      self.t = (log.log["value"][0] + 1) * 2
      self.log(INFO, "sleeping for: %d" % self.t)
    time.sleep(self.t)
    self.return_query_res(port, ret_log)