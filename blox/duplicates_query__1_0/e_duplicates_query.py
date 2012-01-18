from element import *
from logging import ERROR, WARN, INFO, DEBUG

class duplicates_query(Element):
  def on_load(self, config):
    self.name = "Duplicate-query"
    self.add_port("query", Port.PULL, Port.UNNAMED, [])
    self.log(INFO, "Duplicate-query element loaded")

  def do_task(self):
    yield
    log = Log()
    res = self.pull("query", log).log
    for i in range(len(res["hash"])):
      self.log(INFO, "%s: " % res["hash"][i])
      for f in res["files"][i]:
        self.log(INFO, "\t%s" % f)