from element import *

class SimplePull(Element):
  name = "Simple-pull"
  
  def on_load(self, config):
    self.name = "Simple-pull"
    self.add_port("input", Port.PULL, Port.UNNAMED, ["number"])
    print "Simple-pull element loaded"

  def recv_pull_query(self, port, log):
    # print "got query " + str(log.log)
    number = log.log["number"]
    print self.name + " got query for number " + str(number)
    ret_log = Log()
    ret_log.log["result"] = number + 1
    self.return_pull(port, ret_log)
