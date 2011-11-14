from element import *

class Shard(Element):
  def __init__(self, master_port_num):
    Element.__init__(self, master_port_num)
    self.num_nodes = 0
    print "Shard: master port %d" % self.master_port.port_number
    
  def minimum_nodes(self):
    raise NotImplementedError

  def node_type(self):
    raise NotImplementedError

  def config_for_new_node(self):
    raise NotImplementedError
    
  def can_add_node(self):
    raise NotImplementedError
  
  def should_add_node(self, node_num):
    raise NotImplementedError
  
  def remove_node(self, node_num):
    raise NotImplementedError
  
  def set_join_node(self, join_node):
    self.join_node = join_node
    
  def port_name(self, port_num):
    return "output" + str(port_num)
  
  def push_node(self, node_num, log):
    port = self.port_name(node_num)
    self.push(port, log)
  
  def process_master(self, control_data):
    control, data = control_data
    if control == "POLL":
      load = json.dumps(1000)
      self.master_port.socket.send(load)
    elif control == "CAN ADD":
      res = self.can_add_node()
      if res:
        message = (res, self.config_for_new_node()) 
      else: 
        message = (res, {})
      self.master_port.socket.send(json.dumps(message))
    elif control == "SHOULD ADD":
      port_number = data["port_number"]
      node_num = self.num_nodes
      self.num_nodes += 1
      self.register_new_node(node_num, port_number)
      self.master_port.socket.send(json.dumps(True))
      self.should_add_node(node_num)
    else:
      print self.name + " Warning ** could not understand master"
  
  def register_new_node(self, node_num, port_number):
    nt = self.node_type()
    port_type = Port.PULL if nt["port_type"] == "PULL" else Port.PUSH
    port = Element.add_port(self, "output"+str(node_num), port_type, Port.UNNAMED, [])
    port.port_numbers = [port_number]
    self.output_ports[port] = 1
    self.ready_output_port(port)
    
  #TODO: make sure join has the output_port
  def add_output_connection(self, output_port_name, port_number):
    self.join_node.add_output_connection(output_port_name, port_number)
  
  def add_output_node_connection(self, output_port_name, connection_port_num):
    Element.add_output_connection(self, output_port_name, connection_port_num)

class DynamicJoin(Element):
  name = "DynamicJoin"
  
  def on_load(self, config):
    self.name = "DynamicJoin"
    self.join_input_port = self.add_port("input", Port.PUSH, Port.UNNAMED, [])
    self.add_port("output", Port.PUSH, Port.UNNAMED, [])
  
  def recv_push(self, port, log):
    nl = Log()
    nl.set_log(log.log)
    self.push("output", nl)
      
  def process_master(self, control_data):
    control, data = control_data
    if control == "POLL":
      load = json.dumps(1000)
      self.master_port.socket.send(load)
    elif control == "ADD JOIN":
      #print self.name + " got ADD JOIN from master"
      self.add_subscriber()
      self.master_port.socket.send(json.dumps(True))
      
  def add_subscriber(self):
    self.input_ports[self.join_input_port] += 1
    
  def set_join_port_num(self, port_number):
    self.add_input_connection("input", port_number)
    # no subscribers yet, but add_input_connection increments the counter
    self.input_ports[self.join_input_port] = 0