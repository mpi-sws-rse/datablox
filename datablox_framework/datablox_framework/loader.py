import os
import sys
import zmq
import json
from element import *
from shard import *

class Master(object):
  def __init__(self):
    self.master_port = 6000
    self.element_classes = []
    self.elements = {}
    self.loads = {}
    self.context = zmq.Context()
    self.port_num_gen = PortNumberGenerator()
    self.load_elements(os.environ["BLOXPATH"])
    self.setup_connections()
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

  def create_element(self, name, config):
    element = None
    for e in self.element_classes:
      if e.name == name:
        element = e

    if element == None:
      print "Could not find element with name " + name
      raise NameError

    self.master_port += 2  
    inst = element(self.master_port, self.port_num_gen)
    inst.on_load(config)
    self.elements[self.master_port] = inst
    
    if isinstance(inst, Shard):
      self.populate_shard(inst)

    return inst

  def populate_shard(self, shard):
    num_elements = shard.minimum_nodes()
    element_name = shard.node_name()
    for i in range(num_elements):
      output_port = "output"+str(i)
      input_port = "input"
      element_config = shard.config_for_node(i)
      shard.add_port(output_port, Port.PUSH, Port.UNNAMED, [])
      e = self.create_element(element_name, element_config)
      shard.connect(output_port, e, input_port)
    
  def start_elements(self):
    for e in self.elements.values():
      print "starting " + e.name
      e.start()
  
  def run(self):
    self.sync_elements()
    try:
      self.poll_loads()
      #self.parallelize()
    except KeyboardInterrupt:
      self.stop_all()
  
  def listen_url(self, port_number):
    return "tcp://localhost:" + str(port_number)

  def sync_elements(self):
    for (p, e) in self.elements.items():
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
    self.loads = {}
    for (p, e) in self.elements.items():
      if e.alive:
        load = self.poll_load(p, e) 
        if load != None:
          self.loads[p] = load
  
  def poll_load(self, port, element):
    message = json.dumps("POLL")
    socket = self.context.socket(zmq.REQ)
    socket.connect(self.listen_url(port))
    socket.send(message)
    #wait for 4 sec
    load = self.timed_recv(socket, 4000)
    if load != None:
      load = json.loads(load)
      print "Master: %s has a load %d" % (element.name, load)
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
    
  def setup_connections(self):
    # crawler1 = self.create_element("Dir-Src", {"directory": "."})
    # crawler2 = self.create_element("Dir-Src", {"directory": "/Users/saideep/Downloads/pylucene-3.4.0-1/samples"})
    # join = self.create_element("Join", {"joins": 2})
    # categorizer = self.create_element("Categorize", {})
    # indexer = self.create_element("Solr-index", {"crawlers": 2})
    # metaindexer = self.create_element("File-mongo", {"crawlers": 2})
    # query = self.create_element("File-query", {})
    # 
    # crawler1.connect("output", join, "input1")
    # crawler2.connect("output", join, "input2")
    # join.connect("output", categorizer, "input")
    # categorizer.connect("output", indexer, "input")
    # categorizer.connect("output", metaindexer, "input")
    # query.connect("meta_query", metaindexer, "file_data")
    # query.connect("data_query", indexer, "query")

    #source1 = self.create_element("0-Src", {"directory": "."})
    #source2 = self.create_element("Dir-Src", {"directory": "/Users/saideep/Downloads/pylucene-3.4.0-1/samples"})
    #source3 = self.create_element("Dir-Src", {"directory": "/Users/saideep/Projects/sandbox"})
    #trans1 = self.create_element("Categorize", {})
    #trans2 = self.create_element("Categorize", {})
    #join1 = self.create_element("Join", {"joins": 2})
    #join2 = self.create_element("Join", {"joins": 2})
    #sink = self.create_element("Solr-index", {"crawlers": 3})
    #sink = self.create_element("Null", {})
    
    # source1.connect("output", trans1, "input")
    # source2.connect("output", join1, "input1")
    # source3.connect("output", join1, "input2")
    # trans1.connect("output", join2, "input1")
    # join1.connect("output", join2, "input2")
    # join2.connect("output", sink, "input")
    #source1.connect("output", sink, "input")

    # source = self.create_element("Simple-pull-client", {})
    # sink = self.create_element("Simple-pull", {})
    # source.connect("output", sink, "input")

    source = self.create_element("0-Src", {"directory": "."})
    sink = self.create_element("Null-Shard", {})
    # sink = self.create_element("Null", {})
    source.connect("output", sink, "input")
  
if __name__ == "__main__":
  m = Master()