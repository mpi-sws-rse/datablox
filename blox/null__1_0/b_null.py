from block import *
import time
from logging import ERROR, WARN, INFO, DEBUG

class null(Block):
  def on_load(self, config):
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.sleep_time = config["sleep"] if config.has_key("sleep") else 0
    self.logs = 0
    self.rows = 0
    self.log(INFO, "NULL block loaded")

  def recv_push(self, port, log):
    self.logs += 1
    self.rows += log.num_rows()
    time.sleep(self.sleep_time)
  
  def on_shutdown(self):
    self.log(INFO, "got %d logs, %d rows" % (self.logs, self.rows))