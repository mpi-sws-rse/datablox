import zmq
import json
import time

import naming
from block import *
from shard import *

class Master(object):
  def __init__(self, bloxpath, config_file, ip_addr_list,
               using_engage, log_level=logging.INFO):
    self.master_port = 6500
    self.block_classes = []
    self.blocks = {}
    self.loads = {}
    self.shard_nodes = {}
    self.num_parallel = 0
    self.ip_pick = 0
    self.log_level = log_level
    self.using_engage = using_engage
    self.ipaddress_hash = self.get_ipaddress_hash(ip_addr_list)
    self.context = zmq.Context()
    self.port_num_gen = PortNumberGenerator()
    self.bloxpath = bloxpath
    self.add_blox_to_path(bloxpath)
    self.setup_connections(config_file)
    self.start_blocks()
    self.run()

  def get_ipaddress_hash(self, ipaddresses):
    d = {}
    for ip in ipaddresses:
      d[ip] = 0
    return d
  
  def select_ipaddress(self):
    #select the node which has the least number of running blocks
    min_ip = (None, 100000)
    for k, v in self.ipaddress_hash.items():
      if min_ip[1] > v:
        min_ip = (k, v)
    #increment the blocks on this one
    self.ipaddress_hash[min_ip[0]] = min_ip[1] + 1
    return min_ip[0]
  
  #TODO: Put all these utility functions in their own module
  def add_blox_to_path(self, blox_dir):
    try:
      sys.path.index(blox_dir)
    except ValueError:
      sys.path.append(blox_dir)
  
  def is_shard(self, block_name):
    return naming.block_class_name(block_name).endswith('_shard')
  
  def block_class(self, block_name, version=naming.DEFAULT_VERSION):
    return naming.get_block_class(block_name, version)
    
  def create_block(self, name, e_id, config, version=naming.DEFAULT_VERSION,
                     pin_ipaddress=None):
    path = naming.block_path(self.bloxpath, name, version)
    if not os.path.isfile(path):
      print "Could not find the block " + path
      raise NameError
    
    if pin_ipaddress == None:
      ipaddress = self.select_ipaddress()
    else:
      print "got an ipaddress for %s, using %s" % (name, pin_ipaddress)
      ipaddress = pin_ipaddress

    self.master_port += 2
    connections = {}
    inst = {"name": name, "id": e_id, "args": config, 
          "connections": connections, "master_port": self.master_port,
          "ipaddress": ipaddress, "timeouts": 0}
    self.blocks[self.master_port] = inst
    #random initial value
    self.loads[self.master_port] = 0
    
    if self.is_shard(name):
      ec = self.block_class(name)
      inst["initial_configs"] = ec.initial_configs(config)
      inst["node_type"] = ec.node_type()
      self.populate_shard(inst)

    return inst

  def shard_block_id(self, shard, block_num):
    return shard["id"] + "-element-" + str(block_num)
  
  def shard_join_id(self, shard):
    return shard["id"] + "-join"
    
  def populate_shard(self, shard):
    block_configs = shard["initial_configs"]
    assert(len(block_configs) > 0)
    block_type = shard["node_type"]
    block_name = block_type["name"]
    input_port = block_type["input_port"]
    print "num blocks in shard %d" % (len(block_configs))
    if block_type.has_key("output_port"):
      #optimization: creating the join block on the same node as the shard
      # TODO: create a unique id for each shard
      join = self.create_block("dynamic-join", self.shard_join_id(shard), {},
                                 pin_ipaddress=shard["ipaddress"])
      join_port_num = self.port_num_gen.new_port()
      join_url = self.url(join["ipaddress"], join_port_num)
      join["join_port_num"] = join_port_num
      join["subscribers"] = 0
      shard["join_node"] = join

    for i in range(len(block_configs)):
      output_port = "output"+str(i)
      block_config = block_configs[i]
      e = self.create_block(block_name, self.shard_block_id(shard, i), block_config)
      self.connect_node(shard, output_port, e, input_port)
      if block_type.has_key("output_port"):
        #TODO: remove hardcoded join input port name
        self.connect_node(e, block_type["output_port"], join, "input", join_url)
        join["subscribers"] += 1
        
  def start_blocks(self):
    for e in self.blocks.values():
      print "starting " + e["name"]
      self.start_block(e)

    success = self.sync_blocks()
    if not success:
      print "Master: Could not start all blocks. Ending the run"
      self.stop_all()
    
  
  def start_block(self, block):
    config = {}
    config["name"] = block["name"]
    config["id"] = block["id"]
    config["args"] = block["args"]
    config["log_level"] = self.log_level
    config["master_port"] = self.url(block["ipaddress"], block["master_port"])
    config["ports"] = block["connections"]
    #for the join block
    if block.has_key("subscribers"):
      config["subscribers"] = block["subscribers"]
    #for the shard block
    if self.is_shard(block["name"]):
      config["num_blocks"] = len(block["initial_configs"])

    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("ADD NODE", config))
    socket.connect(self.url(block["ipaddress"], 5000))
    socket.send(message)
    print "waiting for caretake to load " + block["name"]
    res = json.loads(socket.recv())
    socket.close()
    if not res:
      print "Could not start block " + block["name"]
      raise NameError
    else:
      print block["name"] + " loaded"
      
  def run(self):
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

  def sync_blocks(self):
    res = True
    for (p, e) in self.blocks.items():
      res = res and self.sync_block(p, e)
    return res

  def sync_block(self, p, e):
    url = self.url(e["ipaddress"], p)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(url)
    print "syncing with block %s at url %s" % (e["name"], url)
    syncclient.send('')
    # wait for synchronization reply
    # TODO: hardcoded wait for 8 seconds
    res = self.timed_recv(syncclient, 8000)
    syncclient.close()
    return False if res == None else True
  
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
      load = self.poll_load(self.blocks[p])
      if load != None and load != -1:
        self.loads[p] = load
        self.blocks[p]["timeouts"] = 0
      elif load == None:
        #give it 3 tries to recover before giving up on it
        #TODO: 3 is hardcoded
        if self.blocks[p]["timeouts"] < 3:
          self.blocks[p]["timeouts"] += 1
          self.loads[p] = -1
  
  def poll_load(self, block):
    port = block["master_port"]
    message = json.dumps(("POLL", {}))
    socket = self.context.socket(zmq.REQ)
    socket.connect(self.url(block["ipaddress"], port))
    socket.send(message)
    #wait for 4 sec
    load = self.timed_recv(socket, 4000)
    socket.close()
    if load != None:
      load = json.loads(load)
      print "Master: %s has served %r (%r)" % (block["name"], load, (load - self.old_loads[block["master_port"]]))
      return load
    #block timed out
    else:
      print "** Master: %s timed out" % block["name"]
      return None

  def stop_all(self):
    print "Master: trying to stop all blocks"
    for ip in self.ipaddress_hash.keys():
      self.stop_one(ip)
    print "done, quitting"
    self.context.term()
    sys.exit(0)
    
  def stop_one(self, ipaddress):
    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("STOP ALL", {}))
    socket.connect(self.url(ipaddress, 5000))
    socket.send(message)
    print "waiting for caretaker at %s to stop all blocks " % ipaddress
    res = json.loads(socket.recv())
    socket.close()
    
  def parallelize(self):
    for p in self.loads.keys():
      e = self.blocks[p]
      if self.is_shard(e["name"]):
        can, config = self.can_parallelize(e)
        if can and self.num_parallel < 4:
          self.do_parallelize(e, config)
  
  def can_parallelize(self, block):
    socket = self.context.socket(zmq.REQ)
    port = block["master_port"]
    socket.connect(self.url(block["ipaddress"], port))
    message = json.dumps(("CAN ADD", {}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    socket.close()
    if message != None:
      return json.loads(message)
    else:
      print "Master did not get any result for parallelize from %s" % block["name"]
      return (False, None)
  
  def do_parallelize(self, shard, config):
    node_type = shard["node_type"]
    new_node = self.create_block(node_type["name"], self.shard_block_id(shard, len(shard["initial_configs"])), config)
    port_number = self.port_num_gen.new_port()
    connection_url = self.url(new_node["ipaddress"], port_number)
    print "Master: trying to parallelize %s with url %s" % (shard["name"], connection_url)
    #TODO: rename initial_configs 
    shard["initial_configs"].append(config)
    self.connect_node(shard, "output"+str(len(shard["initial_configs"])), new_node, node_type["input_port"], connection_url)
    if shard.has_key("join_node"):
      join = shard["join_node"]
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
    self.start_block(new_node)
    success = self.sync_block(new_node["master_port"], new_node)
    if not success:
      print "Master: New block did not synchronize, not parallelizing"
      return

    socket = self.context.socket(zmq.REQ)
    port = shard["master_port"]
    socket.connect(self.url(shard["ipaddress"], port))
    message = json.dumps(("SHOULD ADD", {"port_url": connection_url}))
    socket.send(message)
    message = self.timed_recv(socket, 8000)
    socket.close()
    if message != None:
      print "Master: done parallelizing " + shard["name"]
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

  def connect_node(self, from_block, from_port, to_block, to_port, connection_url=None):
    if connection_url == None:
      connection_port_num = self.port_num_gen.new_port()
      connection_url = self.url(to_block["ipaddress"], connection_port_num)
    from_connections = self.get_or_default(from_block["connections"], from_port, ["output"])
    from_connections.append(connection_url)
    to_connections = self.get_or_default(to_block["connections"], to_port, ["input"])
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
    for e in config["blocks"]:
      if e.has_key("at"):
        pin_ipaddress = e["at"]
        self.ipaddress_hash[pin_ipaddress] += 1
    
  def setup_connections(self, file_name):
    with open(file_name) as f:
      config = json.load(f)
    self.setup_initial_node_counts(config)
    block_hash = {}
    for e in config["blocks"]:
      block_id = e["id"] if e.has_key("id") else None
      block_name = e["name"] 
      block_config = e["args"]
      block_ip = e["at"] if e.has_key("at") else None
      block_version = e["version"] if e.has_key("version") \
                                     else naming.DEFAULT_VERSION
      if self.using_engage:
        resource_key = naming.get_block_resource_key(block_name,
                                                     block_version)
        datablox_engage_adapter.install.install_block(resource_key)
      block = self.create_block(block_name, block_id, block_config,
                                    pin_ipaddress=block_ip)
      block_hash[block_id] = block
    
    for f, t in config["connections"]:
      (from_name, from_port) = self.get_single_item(f)
      (to_name, to_port)  = self.get_single_item(t)
      from_block = block_hash[from_name]
      #if we have a shard, connect the join node instead
      #TODO: hardcoded join output port name
      if from_block.has_key("join_node"):
        from_block = from_block["join_node"]
        from_port = "output"
      to_block = block_hash[to_name]
      self.connect_node(from_block, from_port, to_block, to_port)