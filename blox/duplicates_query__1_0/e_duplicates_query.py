from element import *

class duplicates_query(Element):
  def on_load(self, config):
    self.name = "Duplicate-query"
    self.add_port("query", Port.PULL, Port.UNNAMED, [])
    print "Duplicate-query element loaded"

  def do_task(self):
    yield
    log = Log()
    res = self.pull("query", log).log
    for i in range(len(res["hash"])):
      print "%s: " % res["hash"][i]
      for f in res["files"][i]:
        print "\t%s" % f