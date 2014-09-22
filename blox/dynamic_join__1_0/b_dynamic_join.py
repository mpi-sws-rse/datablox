from block import *
from logging import ERROR, WARN, INFO, DEBUG

class dynamic_join(Block):
  def on_load(self, config):
    self.join_input_port = self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("output", Port.PUSH, Port.UNNAMED, [])
    self.token_counts = {}
    self.subscriber_hwm = 0 # high water mark, used to count completed tokens
  
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      token = log.log['token'][0]
      self.log(INFO, "Received token %s" % token)
      if token in self.token_counts:
        self.token_counts[token] += 1
      else:
        self.token_counts[token] = 1
      tc = self.token_counts[token]
      if tc<self.subscriber_hwm:
        self.log(INFO, "Received %d tokens, waiting for %d more tokens from shards" %
                       (tc, self.subscriber_hwm-tc))
        return
      else:
        assert tc==self.subscriber_hwm
        self.log(INFO, "Received all %d tokens for %s, will pass on to output port" % (self.subscriber_hwm, token))
    nl = Log()
    nl.set_log(log.log)
    self.push("output", nl)
      
  def process_master(self, control, data):
    if control == "ADD JOIN":
      self.add_subscriber()
      self.master_port.socket.send(json.dumps(True))
      
  def add_subscriber(self):
    self.input_ports[self.join_input_port] += 1
    self.subscriber_hwm += 1
    self.log(INFO, "add subscriber: num input ports = %d" % self.input_ports[self.join_input_port])
  
  def set_subscribers(self, num_sub):
    self.input_ports[self.join_input_port] = num_sub
    self.subscriber_hwm = num_sub
    self.log(INFO, "set_subscribers(%d)" % num_sub)
    
  def set_join_port_num(self, port_number):
    self.add_input_connection("input", port_number)
    # no subscribers yet, but add_input_connection increments the counter
    self.input_ports[self.join_input_port] = 0
