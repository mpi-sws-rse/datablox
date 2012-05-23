from block import *
from logging import ERROR, WARN, INFO, DEBUG

class simple_query_async_client(Block):
  def on_load(self, config):
    self.add_port("output", Port.QUERY, Port.UNNAMED, ["value"])
    self.log(INFO, "Simple-query-async-client block loaded")

  def recv_query_result(self, port, res):
    values = res.log["result"]
    self.log(INFO, self.id + " got result " + str(values))

  def do_task(self):
    log = Log()
    log.log["value"] = [23]
    self.query("output", log, async=True)
    yield
