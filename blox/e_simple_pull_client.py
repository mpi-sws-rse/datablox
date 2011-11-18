from element import *

class simple_pull_client(Element):
  def on_load(self, config):
    self.name = "Simple-pull-client"
    self.add_port("output", Port.PULL, Port.UNNAMED, ["number"])
    print "Simple-pull-client element loaded"

  def src_start(self):
    log = Log()
    log.log["number"] = 23
    res = self.pull("output", log)
    number = res.log["result"]
    print self.name + " got result " + str(number)
    self.shutdown()
