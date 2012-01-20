from block import *
from logging import ERROR, WARN, INFO, DEBUG

class dynamic_join(Block):
  def on_load(self, config):
    self.join_input_port = self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("output", Port.PUSH, Port.UNNAMED, [])
  
  def recv_push(self, port, log):
    nl = Log()
    nl.set_log(log.log)
    self.push("output", nl)
      
  def process_master(self, control, data):
    if control == "POLL":
      load = json.dumps(self.get_load())
      self.master_port.socket.send(load)
    elif control == "ADD JOIN":
      self.add_subscriber()
      self.master_port.socket.send(json.dumps(True))
      
  def add_subscriber(self):
    self.input_ports[self.join_input_port] += 1
  
  def set_subscribers(self, num_sub):
    self.input_ports[self.join_input_port] = num_sub
    
  def set_join_port_num(self, port_number):
    self.add_input_connection("input", port_number)
    # no subscribers yet, but add_input_connection increments the counter
    self.input_ports[self.join_input_port] = 0