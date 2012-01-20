from block import *
from logging import ERROR, WARN, INFO, DEBUG

class duplicates_query(Block):
  def on_load(self, config):
    self.name = "Duplicate-query"
    self.add_port("query", Port.QUERY, Port.UNNAMED, [])
    self.log(INFO, "Duplicate-query block loaded")

  def do_task(self):
    yield
    log = Log()
    res = self.query("query", log).log
    for i in range(len(res["hash"])):
      self.log(INFO, "%s: " % res["hash"][i])
      for f in res["files"][i]:
        self.log(INFO, "\t%s" % f)