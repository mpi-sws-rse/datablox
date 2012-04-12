import sys
import os.path
from logging import ERROR, WARN, INFO, DEBUG
import time

try:
  import datablox_framework
except ImportError:
  sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                               "../../datablox_framework")))
  import datablox_framework

from datablox_framework.block import *

class zero_src(Block):
  def do_task(self):
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    numzeros = self.config["num_zeros"] if self.config.has_key("num_zeros") else 10
    for i in range(0,numzeros):
      log = Log()
      log.log["value"] = [0]
      self.log(INFO, "Sending a zero (%d)" % i)
      self.push("output", log)
      time.sleep(sleeptime)
      yield

  def on_load(self, config):
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["value"])
    self.log(INFO, "0-Src block loaded")
