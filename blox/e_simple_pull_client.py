from element import *

class SimplePullClient(Element):
  name = "Simple-pull-client"
  
  def on_load(self, config):
    self.name = "Simple-pull-client"
    self.add_port("output", Port.PULL, Port.UNNAMED, ["number"])
    print "Simple-pull-client element loaded"

  def src_start(self):
    log = Log()
    log.log["number"] = 23
    self.pull("output", log)
  
  def recv_pull_result(self, port, log):
    number = log.log["result"]
    print self.name + " got result " + str(number)
    self.shutdown()
