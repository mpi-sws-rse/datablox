from element import *

class Shard(Element):
  def minimum_nodes(self):
    raise NotImplementedError

  def node_name(self):
    raise NotImplementedError

  def config_for_node(self, node_num):
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
  
  def process_master(self, control_data):
    if control_data == "POLL":
      print "Shard element got something from master"
      load = json.dumps(1000)
      self.master_port.socket.send(load)
    else:
      print self.name + " Warning ** could not understand master"