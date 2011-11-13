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
      self.register_new_node(port_number)
      self.master_port.socket.send(json.dumps(True))
    else:
      print self.name + " Warning ** could not understand master"
  
  def process_control(self, control_port, control_data):
    print "got some control"
    if control_data == "READY":
      print self.name + " got a new node"
      
      node_num = self.num_nodes
      self.num_nodes += 1
      port_number = control_port.port_number - 1
      #TODO: figure out the port type by adding a property to shard
      port = Element.add_port(self, "output"+str(node_num), Port.PUSH, Port.UNNAMED, [])
      port.port_number = port_number
      port.end_point = self
      self.bind_pub_port(port)
      self.ports.append(port)
      self.output_ports[port] = 1

      ack_message = json.dumps(("CTRL", "ACK"))
      control_port.socket.send(ack_message)
      #we are done with this port
      control_port.socket.close()
      self.ports.remove(control_port)
      #now tell the object that it has a new node
      self.should_add_node(node_num)
  
  def register_new_node(self, port_number):
    control_port = Port("ctl"+str(port_number), Port.CONTROL, Port.UNNAMED, [], port_number + 1)
    control_port.end_point = self
    self.ports.append(control_port)
    control_url = self.bind_url(port_number+1)
    control_port.socket = self.context.socket(zmq.REP)
    control_port.socket.bind(control_url)
    self.control_poller.register(control_port.socket)
    # # wait for synchronization request
    # msg = control_port.socket.recv()
    # # send synchronization reply
    # ack_message = json.dumps(("CTRL", "ACK"))
    # control_port.socket.send(ack_message)
    # print self.name + " +1 subscriber"
    
    
  def shutdown(self):
    control_ports = [p for p in self.ports if p.port_type == Port.CONTROL]
    for p in control_ports:
      #wait for this element to connect
      p.socket.recv()
    #now that they're all listening, tell them to END
    Element.shutdown(self)

  def get_output_port_num(self, port_name):
    return self.join_node.get_output_port_num(port_name)
    
  #TODO: make sure join has the output_port
  def add_output_connection(self, output_port_name, port_number):
    self.join_node.connect(output_port_name, element, input_port_name)
  
  def add_output_node_connection(self, output_port_name, connection_port_num):
    Element.add_output_connection(self, output_port_name, connection_port_num)