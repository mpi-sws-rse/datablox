from block import *
from logging import ERROR, WARN, INFO, DEBUG

class bookmark_client(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["internet_url"])
    self.urls = config["urls"]

  def do_task(self):
    log = Log()
    log.append_field("internet_url", self.urls)
    self.push("output", log)
    yield