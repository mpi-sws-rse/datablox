from block import *
import os
import time
import base64
from logging import ERROR, WARN, INFO, DEBUG

class file_data_reader(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["url"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["url", "data"])

  def get_data(self, log):
    return [base64.b64encode(BlockUtils.fetch_file_at_url(u)) for u in log["url"]]
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.id + " got the finish token for directory " + log.log["token"][0])
    else:
      log.append_field("data", self.get_data(log.log))

    self.buffered_push("output", log)
    if self.config.has_key("sleep"):
      time.sleep(self.config["sleep"])
