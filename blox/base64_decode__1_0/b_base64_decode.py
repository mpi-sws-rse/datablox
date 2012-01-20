from block import *
import time
import base64
from logging import ERROR, WARN, INFO, DEBUG

class base64_decode(Block):
  def on_load(self, config):
    self.name = "base64_decode"
    self.config = config
    if not config.has_key("fields"):
      self.log(ERROR, "base64_decode should know which fields to decode")
      raise KeyError
    else:
      self.keys = config["fields"]
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("output", Port.PUSH, Port.UNNAMED, [])
    self.log(INFO, "Base64-Decode block loaded")

  def recv_push(self, port, log):
    for key in self.keys:
      if not log.log.has_key(key):
        continue
      else:
        values = log.log[key]
        values_decoded = [base64.b64decode(v) for v in values]
        log.append_field(key, values_decoded)
    self.push("output", log)