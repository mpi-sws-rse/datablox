from block import *
from logging import ERROR, WARN, INFO, DEBUG

class simple_query_client(Block):
  def on_load(self, config):
    self.name = "Simple-query-client"
    self.add_port("output", Port.QUERY, Port.UNNAMED, ["number"])
    self.log(INFO, "Simple-query-client block loaded")

  def do_task(self):
    log = Log()
    log.log["number"] = 23
    res = self.query("output", log)
    number = res.log["result"]
    self.log(INFO, self.name + " got result " + str(number))
    yield
