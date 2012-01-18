from element import *
from logging import ERROR, WARN, INFO, DEBUG

class simple_pull_client(Element):
  def on_load(self, config):
    self.name = "Simple-pull-client"
    self.add_port("output", Port.PULL, Port.UNNAMED, ["number"])
    self.log(INFO, "Simple-pull-client element loaded")

  def do_task(self):
    log = Log()
    log.log["number"] = 23
    res = self.pull("output", log)
    number = res.log["result"]
    self.log(INFO, self.name + " got result " + str(number))
    yield
