import os
import sys
import zmq
import json
import time
from element import *
from shard import *

class Master(object):
  def __init__(self, config_file):
    self.master_port = 6000
    self.element_classes = []
    self.elements = {}
    self.loads = {}
    self.shard_nodes = {}
    self.num_parallel = 0
    self.context = zmq.Context()
    self.port_num_gen = PortNumberGenerator()
    self.load_elements(os.environ["BLOXPATH"])
    self.setup_connections(config_file)
    self.start_elements()
    self.run()


  def load_elements(self, path):
    try:
      sys.path.index(path)
    except ValueError:
      sys.path.append(path)
    for name in os.listdir(path):
      if name.endswith(".py") and name.startswith("e_"):
        modulename = os.path.splitext(name)[0]
        print "importing: " + modulename
        __import__(modulename)
  
    self.element_classes = Element.__subclasses__()
    self.element_classes.extend(Shard.__subclasses__())
    #self.element_classes.append(DynamicJoin)

  def create_element(self, name, config):
    element = None
    for e in self.element_classes:
      if e.name == name:
        element = e

    if element == None:
      print "Could not find element with name " + name
      raise NameError

    self.master_port += 2  
    inst = element(self.master_port)
    inst.on_load(config)
    self.elements[self.master_port] = inst
    #random initial value
    self.loads[inst] = 1000

    if isinstance(inst, Shard):
      self.populate_shard(inst)

    return inst

  def populate_shard(self, shard):
    num_elements = shard.minimum_nodes()
    shard.num_nodes = num_elements
    element_type = shard.node_type()
    self.shard_nodes[shard] = element_type
    element_name = element_type["name"]
    input_port = element_type["input_port"]
    if element_type.has_key("output_port"):
      join = self.create_element("DynamicJoin", {})
      join_port_num = self.port_num_gen.new_port()
      join.set_join_port_num(join_port_num)
      self.shard_nodes[shard]["join_node"] = join
      shard.set_join_node(join)

    for i in range(num_elements):
      output_port = "output"+str(i)
      element_config = shard.config_for_new_node()
      e = self.create_element(element_name, element_config)
      connection_port_num = self.port_num_gen.new_port()
      shard.add_port(output_port, Port.PUSH, Port.UNNAMED, [])
      shard.add_output_node_connection(output_port, connection_port_num)
      e.add_input_connection(input_port, connection_port_num)
      if element_type.has_key("output_port"):
        e.add_output_connection(element_type["output_port"], join_port_num)
        join.add_subscriber()

    # for i in range(num_elements):
    #   output_port = "output"+str(i)
    #   element_config = shard.config_for_new_node()
    #   shard.add_port(output_port, Port.PUSH, Port.UNNAMED, [])
    #   e = self.create_element(element_name, element_config)
    #   connection_port_num = self.port_num_gen.new_port()
    #   shard.add_output_node_connection(output_port, connection_port_num)
    #   e.add_input_connection(input_port, connection_port_num)
    #   if element_type.has_key("output_port"):
    #     self.connect(e, element_type["output_port"], join, "input"+str(i+1))
        
  def start_elements(self):
    for e in self.elements.values():
      print "starting " + e.name
      e.start()
  
  def run(self):
    self.sync_elements()
    while True:
      try:
        self.poll_loads()
        if len(self.loads.keys()) == 0:
          print "Master: no more running nodes, quitting"
          break
        self.parallelize()
        time.sleep(1)
      except KeyboardInterrupt:
        self.stop_all()
        break
  
  def listen_url(self, port_number):
    return "tcp://localhost:" + str(port_number)

  def sync_elements(self):
    for (p, e) in self.elements.items():
      self.sync_element(p, e)

  def sync_element(self, p, e):
    url = self.listen_url(p)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(url)
    syncclient.send('')
    # wait for synchronization reply
    syncclient.recv()
  
  def timed_recv(self, socket, time):
    """time is to be given in milliseconds"""
    poller = zmq.Poller()
    poller.register(socket)
    socks = dict(poller.poll(time))
    if socks == {} or socks[socket] != zmq.POLLIN:
      return None
    else:
      return socket.recv()
    
  def poll_loads(self):
    elements = self.loads.keys()
    self.loads = {}
    for e in elements:
      load = self.poll_load(e)
      if load != None and load != -1:
        self.loads[e] = load
  
  def poll_load(self, element):
    if element.name == "Dir-Src:./blox":
      return None
    port = element.master_port.port_number
    message = json.dumps(("POLL", {}))
    socket = self.context.socket(zmq.REQ)
    socket.connect(self.listen_url(port))
    socket.send(message)
    #wait for 4 sec
    load = self.timed_recv(socket, 4000)
    if load != None:
      load = json.loads(load)
      #print "Master: %s has a load %r" % (element.name, load)
      return load
    #element timed out
    else:
      print "** Master: %s timed out" % element.name
      return None

  def stop_all(self):
    print "Master: trying to stop all elements"
    #not thread-safe, implement this better
    for e in self.elements.values():
      e.alive = False
    
  def parallelize(self):
    for e in self.loads.keys():
      if isinstance(e, Shard):
        can, config = self.can_parallelize(e)
        if can and self.num_parallel < 4:
          self.do_parallelize(e, config)
  
  def can_parallelize(self, element):
    socket = self.context.socket(zmq.REQ)
    port = element.master_port.port_number
    socket.connect(self.listen_url(port))
    message = json.dumps(("CAN ADD", {}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    if message != None:
      return json.loads(message)
    else:
      print "Master did not get any result for parallelize from %s" % element.name
      return (False, None)
  
  def do_parallelize(self, element, config):
    print "Master: trying to parallelize %s" % element.name
    port_number = self.port_num_gen.new_port()
    print "Master: %s can parallelize with config %r, on port %r" % (element.name, config, port_number)
    node_type = self.shard_nodes[element]
    new_node = self.create_element(node_type["name"], config)
    new_node.add_input_connection(node_type["input_port"], port_number)
    if node_type.has_key("join_node"):
      join = node_type["join_node"]
      new_node.add_output_connection(node_type["output_port"], join.join_input_port.port_number)
      socket = self.context.socket(zmq.REQ)
      socket.connect(self.listen_url(join.master_port.port_number))
      message = json.dumps(("ADD JOIN", {}))
      socket.send(message)
      res = self.timed_recv(socket, 8000)
      if message == None:
        print "join node did not reply to add join, so not parallelizing"
        return      
    new_node.start()
    self.sync_element(new_node.master_port.port_number, new_node)

    socket = self.context.socket(zmq.REQ)
    port = element.master_port.port_number
    socket.connect(self.listen_url(port))
    message = json.dumps(("SHOULD ADD", {"port_number": port_number}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    if message != None:
      print "Master: done parallelizing " + element.name
      self.num_parallel += 1
    else:
      print "Master didn't get a reply for should_add"
    
  def get_single_item(self, d):
    items = d.items()
    assert(len(items) == 1)
    return items[0]

  def connect_node(self, from_element, from_port, to_element, to_port):
    connection_port_num = self.port_num_gen.new_port()
    from_element.add_output_connection(from_port, connection_port_num)
    to_element.add_input_connection(to_port, connection_port_num)
    
  def setup_connections(self, file_name):
    with open(file_name) as f:
      config = json.load(f)
    element_hash = {}
    for e in config["elements"]:
      element_id, (element_name, element_config) = self.get_single_item(e)
      element = self.create_element(element_name, element_config)
      element_hash[element_id] = element
    
    for f, t in config["connections"]:
      (from_name, from_port) = self.get_single_item(f)
      (to_name, to_port)  = self.get_single_item(t)
      from_element = element_hash[from_name]
      to_element = element_hash[to_name]
      self.connect_node(from_element, from_port, to_element, to_port)
  
if __name__ == "__main__":
  m = Master(sys.argv[1])