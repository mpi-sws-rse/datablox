from block import *

class Shard(Block):
  def __init__(self, master_port_num):
    Block.__init__(self, master_port_num)
    self.num_nodes = 0
    
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
  
  def port_name(self, port_num):
    return "output" + str(port_num)
  
  def push_node(self, node_num, log):
    port = self.port_name(node_num)
    self.push(port, log)
  
  def process_master(self, control, data):
    if control == "POLL":
      rm, rs = self.get_load()
      load = json.dumps(("ALIVE", rm, rs))
      self.master_port.socket.send(load)
    elif control == "CAN ADD":
      res = self.can_add_node()
      if res:
        message = (res, self.config_for_new_node()) 
      else: 
        message = (res, {})
      self.master_port.socket.send(json.dumps(message))
    elif control == "SHOULD ADD":
      port_url = data["port_url"]
      node_num = self.num_nodes
      self.num_nodes += 1
      self.register_new_node(node_num, port_url)
      self.master_port.socket.send(json.dumps(True))
      self.should_add_node(node_num)
    else:
      print self.id + " Warning ** could not understand master"
  
  def register_new_node(self, node_num, port_url):
    nt = self.node_type()
    port_type = Port.QUERY if nt["port_type"] == "QUERY" else Port.PUSH
    print self.id + " adding port " + "output"+str(node_num) + " with url " + port_url
    port = Block.add_port(self, "output"+str(node_num), port_type, Port.UNNAMED, [])
    port.port_urls = [port_url]
    self.output_ports[port] = 1
    self.ready_output_port(port)
    
  # def add_output_connection(self, output_port_name, connection_port_num):
  #   Block.add_output_connection(self, output_port_name, connection_port_num)