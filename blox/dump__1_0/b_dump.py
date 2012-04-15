from block import *
import time
import base64
from logging import ERROR, WARN, INFO, DEBUG

class dump(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("rpc", Port.QUERY, Port.UNNAMED, [])
    self.sleep_time = config["sleep"] if config.has_key("sleep") else 0
    self.keys = config["decode_fields"] if config.has_key("decode_fields") else []
    self.log(INFO, "Dump block loaded")

  def decode_fields(self, log):
    for key in self.keys:
      if not log.log.has_key(key):
        continue
      else:
        values = log.log[key]
        values_decoded = [base64.b64decode(v) for v in values]
        log.append_field(key, values_decoded)
  
  def recv_push(self, port, log):
    self.decode_fields(log)
    self.log(INFO, "log is: " + str(log.log))
    time.sleep(self.sleep_time)
    #yield
  
  def recv_query(self, port, log):
    self.decode_fields(log)
    self.log(INFO, "log is: " + str(log.log))
    #returns True
    ret = Log()
    ret.log["result"] = True
    self.return_query_res(port, ret)