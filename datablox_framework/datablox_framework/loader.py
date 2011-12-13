import os
import sys
import zmq
import json
import time
from optparse import OptionParser

from element import *
from shard import *
import naming

class Master(object):
  def __init__(self, bloxpath, config_file, ip_addr_list):
    self.master_port = 6500
    self.element_classes = []
    self.elements = {}
    self.loads = {}
    self.shard_nodes = {}
    self.num_parallel = 0
    self.ip_pick = 0
    self.ipaddress_hash = self.get_ipaddress_hash(ip_addr_list)
    self.context = zmq.Context()
    self.port_num_gen = PortNumberGenerator()
    self.bloxpath = bloxpath
    self.add_blox_to_path(bloxpath)
    self.setup_connections(config_file)
    self.start_elements()
    self.run()

  #TODO: Fix this
  def get_ipaddress_hash(self, ipaddresses):
    #ipaddresses = ["139.19.157.13", "139.19.192.14", "139.19.193.85", "139.19.157.14", "139.19.157.15"]
    #ipaddresses = ["139.19.157.13", "139.19.157.14", "139.19.157.15"]
    #ipaddresses = ["127.0.0.1"]
    d = {}
    for ip in ipaddresses:
      d[ip] = 0
    return d
  
  def select_ipaddress(self):
    #select the node which has the least number of running elements
    min_ip = (None, 100000)
    for k, v in self.ipaddress_hash.items():
      if min_ip[1] > v:
        min_ip = (k, v)
    #increment the elements on this one
    self.ipaddress_hash[min_ip[0]] = min_ip[1] + 1
    return min_ip[0]
  
  #TODO: Put all these utility functions in their own module
  def add_blox_to_path(self, blox_dir):
    try:
      sys.path.index(blox_dir)
    except ValueError:
      sys.path.append(blox_dir)
  
  def is_shard(self, element_name):
    return naming.element_class_name(element_name).endswith('_shard')
  
  def element_class(self, element_name, version=naming.DEFAULT_VERSION):
    return naming.get_element_class(element_name, version)
    
  def create_element(self, name, config, version=naming.DEFAULT_VERSION,
                     pin_ipaddress=None):
    path = naming.element_path(self.bloxpath, name, version)
    if not os.path.isfile(path):
      print "Could not find the element " + path
      raise NameError
    
    if pin_ipaddress == None:
      ipaddress = self.select_ipaddress()
    else:
      print "got an ipaddress for %s, using %s" % (name, pin_ipaddress)
      ipaddress = pin_ipaddress

    self.master_port += 2
    connections = {}
    inst = {"name": name, "args": config, 
          "connections": connections, "master_port": self.master_port,
          "ipaddress": ipaddress, "timeouts": 0}
    self.elements[self.master_port] = inst
    #random initial value
    self.loads[self.master_port] = 0
    
    if self.is_shard(name):
      ec = self.element_class(name)
      inst["initial_configs"] = ec.initial_configs(config)
      inst["node_type"] = ec.node_type()
      self.populate_shard(inst)

    return inst

  def populate_shard(self, shard):
    element_configs = shard["initial_configs"]
    assert(len(element_configs) > 0)
    element_type = shard["node_type"]
    element_name = element_type["name"]
    input_port = element_type["input_port"]
    print "num elements in shard %d" % (len(element_configs))
    if element_type.has_key("output_port"):
      #optimization: creating the join element on the same node as the shard
      join = self.create_element("dynamic-join", {},
                                 pin_ipaddress=shard["ipaddress"])
      join_port_num = self.port_num_gen.new_port()
      join_url = self.url(join["ipaddress"], join_port_num)
      join["join_port_num"] = join_port_num
      join["subscribers"] = 0
      shard["join_node"] = join

    for i in range(len(element_configs)):
      output_port = "output"+str(i)
      element_config = element_configs[i]
      e = self.create_element(element_name, element_config)
      self.connect_node(shard, output_port, e, input_port)
      if element_type.has_key("output_port"):
        #TODO: remove hardcoded join input port name
        self.connect_node(e, element_type["output_port"], join, "input", join_url)
        join["subscribers"] += 1
        
  def start_elements(self):
    for e in self.elements.values():
      print "starting " + e["name"]
      self.start_element(e)
  
  def start_element(self, element):
    config = {}
    config["name"] = element["name"]
    config["args"] = element["args"]
    config["master_port"] = self.url(element["ipaddress"], element["master_port"])
    config["ports"] = element["connections"]
    #for the join element
    if element.has_key("subscribers"):
      config["subscribers"] = element["subscribers"]
    #for the shard element
    if self.is_shard(element["name"]):
      config["num_elements"] = len(element["initial_configs"])

    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("ADD NODE", config))
    socket.connect(self.url(element["ipaddress"], 5000))
    socket.send(message)
    print "waiting for caretake to load " + element["name"]
    res = json.loads(socket.recv())
    socket.close()
    if not res:
      print "Could not start element " + element["name"]
      raise NameError
    else:
      print element["name"] + " loaded"
      
  def run(self):
    self.sync_elements()
    while True:
      try:
        self.poll_loads()
        if len(self.loads.keys()) == 0:
          print "Master: no more running nodes, quitting"
          return
        self.parallelize()
        #todo: hard coded 10
        time.sleep(10)
      except KeyboardInterrupt:
        self.stop_all()
        break
  
  def url(self, ip_address, port_number):
    return "tcp://" + ip_address + ":" + str(port_number)

  def sync_elements(self):
    for (p, e) in self.elements.items():
      self.sync_element(p, e)

  def sync_element(self, p, e):
    url = self.url(e["ipaddress"], p)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(url)
    print "syncing with element %s at url %s" % (e["name"], url)
    syncclient.send('')
    # wait for synchronization reply
    syncclient.recv()
    syncclient.close()
  
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
    ports = self.loads.keys()
    self.old_loads = self.loads
    self.loads = {}
    for p in ports:
      load = self.poll_load(self.elements[p])
      if load != None and load != -1:
        self.loads[p] = load
        self.elements[p]["timeouts"] = 0
      elif load == None:
        #give it 3 tries to recover before giving up on it
        #TODO: 3 is hardcoded
        if self.elements[p]["timeouts"] < 3:
          self.elements[p]["timeouts"] += 1
          self.loads[p] = -1
  
  def poll_load(self, element):
    port = element["master_port"]
    message = json.dumps(("POLL", {}))
    socket = self.context.socket(zmq.REQ)
    socket.connect(self.url(element["ipaddress"], port))
    socket.send(message)
    #wait for 4 sec
    load = self.timed_recv(socket, 4000)
    socket.close()
    if load != None:
      load = json.loads(load)
      print "Master: %s has served %r (%r)" % (element["name"], load, (load - self.old_loads[element["master_port"]]))
      return load
    #element timed out
    else:
      print "** Master: %s timed out" % element["name"]
      return None

  def stop_all(self):
    print "Master: trying to stop all elements"
    for ip in self.ipaddress_hash.keys():
      self.stop_one(ip)
    print "done, quitting"
    
  def stop_one(self, ipaddress):
    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("STOP ALL", {}))
    socket.connect(self.url(ipaddress, 5000))
    socket.send(message)
    print "waiting for caretaker at %s to stop all elements " % ipaddress
    res = json.loads(socket.recv())
    socket.close()
    
  def parallelize(self):
    for p in self.loads.keys():
      e = self.elements[p]
      if self.is_shard(e["name"]):
        can, config = self.can_parallelize(e)
        if can and self.num_parallel < 4:
          self.do_parallelize(e, config)
  
  def can_parallelize(self, element):
    socket = self.context.socket(zmq.REQ)
    port = element["master_port"]
    socket.connect(self.url(element["ipaddress"], port))
    message = json.dumps(("CAN ADD", {}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    socket.close()
    if message != None:
      return json.loads(message)
    else:
      print "Master did not get any result for parallelize from %s" % element["name"]
      return (False, None)
  
  def do_parallelize(self, element, config):
    node_type = element["node_type"]
    new_node = self.create_element(node_type["name"], config)
    port_number = self.port_num_gen.new_port()
    connection_url = self.url(new_node["ipaddress"], port_number)
    print "Master: trying to parallelize %s with url %s" % (element["name"], connection_url)
    #TODO: rename initial_configs 
    element["initial_configs"].append(config)
    self.connect_node(element, "output"+str(len(element["initial_configs"])), new_node, node_type["input_port"], connection_url)
    if element.has_key("join_node"):
      join = element["join_node"]
      join_url = self.url(join["ipaddress"], join["join_port_num"])
      #TODO: hardcoded join input port
      self.connect_node(new_node, node_type["output_port"], join, "input", join_url)
      # new_node.add_output_connection(node_type["output_port"], join.join_input_port.port_number)
      socket = self.context.socket(zmq.REQ)
      socket.connect(self.url(join["ipaddress"], join["master_port"]))
      message = json.dumps(("ADD JOIN", {}))
      socket.send(message)
      res = self.timed_recv(socket, 8000)
      socket.close()
      if message == None:
        print "join node did not reply to add join, so not parallelizing"
        return      
    self.start_element(new_node)
    self.sync_element(new_node["master_port"], new_node)

    socket = self.context.socket(zmq.REQ)
    port = element["master_port"]
    socket.connect(self.url(element["ipaddress"], port))
    message = json.dumps(("SHOULD ADD", {"port_url": connection_url}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    socket.close()
    if message != None:
      print "Master: done parallelizing " + element["name"]
      self.num_parallel += 1
    else:
      print "Master didn't get a reply for should_add"
    
  def get_single_item(self, d):
    items = d.items()
    assert(len(items) == 1)
    return items[0]
  
  def get_or_default(self, d, key, default):
    if d.has_key(key):
      return d[key]
    else:
      d[key] = default
      return default

  def connect_node(self, from_element, from_port, to_element, to_port, connection_url=None):
    if connection_url == None:
      connection_port_num = self.port_num_gen.new_port()
      connection_url = self.url(to_element["ipaddress"], connection_port_num)
    from_connections = self.get_or_default(from_element["connections"], from_port, ["output"])
    from_connections.append(connection_url)
    to_connections = self.get_or_default(to_element["connections"], to_port, ["input"])
    if len(to_connections) > 1:
      try:
        #it's ok for join url to have multiple inputs
        to_connections.index(connection_url)
        pass
      except ValueError:
        print "Cannot add multiple input connections"
        raise NameError
    else:
      to_connections.append(connection_url)
  
  def setup_initial_node_counts(self, config):
    for e in config["elements"]:
      if e.has_key("at"):
        pin_ipaddress = e["at"]
        self.ipaddress_hash[pin_ipaddress] += 1
    
  def setup_connections(self, file_name):
    with open(file_name) as f:
      config = json.load(f)
    self.setup_initial_node_counts(config)
    element_hash = {}
    for e in config["elements"]:
      element_id = e["id"]
      element_name = e["name"] 
      element_config = e["args"]
      element_ip = e["at"] if e.has_key("at") else None
      element = self.create_element(element_name, element_config,
                                    pin_ipaddress=element_ip)
      element_hash[element_id] = element
    
    for f, t in config["connections"]:
      (from_name, from_port) = self.get_single_item(f)
      (to_name, to_port)  = self.get_single_item(t)
      from_element = element_hash[from_name]
      #if we have a shard, connect the join node instead
      #TODO: hardcoded join output port name
      if from_element.has_key("join_node"):
        from_element = from_element["join_node"]
        from_port = "output"
      to_element = element_hash[to_name]
      self.connect_node(from_element, from_port, to_element, to_port)

def main(argv):
  usage = "%prog [options] config_file ip_address1 ip_address2 ..."
  parser = OptionParser(usage=usage)
  parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                    help="use this path instead of the environment variable BLOXPATH")
                    
  (options, args) = parser.parse_args(argv)

  if len(args)<2:
    parser.error("Need to specify config_file and at least one ip address")

  bloxpath = options.bloxpath
  
  if bloxpath == None: 
    if not os.environ.has_key("BLOXPATH"):
      parser.error("Need to set BLOXPATH environment variable or pass it as an argument")
    else:
      bloxpath = os.environ["BLOXPATH"]

  if not os.path.isdir(bloxpath):
    parser.error("BLOXPATH %s does not exist or is not a directory" % bloxpath)
    
  Master(bloxpath, args[0], args[1:])

def call_from_console_script():
    sys.exit(main(sys.argv[1:]))

if __name__ == "__main__":
  main(sys.argv[1:])
