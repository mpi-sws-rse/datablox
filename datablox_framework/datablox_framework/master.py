import zmq
import json
import time
import copy
import subprocess
import logging

import naming
from block import *
from shard import *

try:
  import datablox_engage_adapter.file_locator
  using_engage = True
except ImportError:
  using_engage = False

if using_engage:
  engage_file_locator = datablox_engage_adapter.file_locator.FileLocator()
  import datablox_engage_adapter.install
  import datablox_engage_adapter.djm_server as djm_server
else:
  engage_file_locator = None

logger = logging.getLogger(__name__)

def get_url(ip_address, port_number):
  return "tcp://" + ip_address + ":" + str(port_number)

def timed_recv(socket, time):
  """time is to be given in milliseconds"""
  poller = zmq.Poller()
  poller.register(socket)
  socks = dict(poller.poll(time))
  if socks == {} or socks[socket] != zmq.POLLIN:
    return None
  else:
    return socket.recv()

def get_or_default(d, key, default):
  if d.has_key(key):
    return d[key]
  else:
    d[key] = default
    return default

def get_single_item(d):
  items = d.items()
  assert(len(items) == 1)
  return items[0]

log_level = logging.INFO
bloxpath = None
global_config = None

def get_group(name):
  for group in global_config:
    if group["group-name"] == name:
      return group
  return None

#TODO: Put all these utility functions in their own module
def add_blox_to_path(blox_dir):
  try:
    sys.path.index(blox_dir)
  except ValueError:
    sys.path.append(blox_dir)

def get_block_class(block_name, version=naming.DEFAULT_VERSION):
  return naming.get_block_class(block_name, version)

def is_shard(block_name):
  return naming.block_class_name(block_name).endswith('_shard')

def is_rpc(block_name):
  return block_name == "RPC"
  
def create_handler(block_record, address_manager, context, policy=None):
  group = get_group(block_record["name"])
  if group == None:
    if is_rpc(block_record["name"]):
      return RPCHandler(block_record, address_manager, context, policy)
    elif is_shard(block_record["name"]):
      return ShardHandler(block_record, address_manager, context, policy)
    else:
      return BlockHandler(block_record, address_manager, context, policy)
  else:
    return GroupHandler(block_record, group, address_manager, context)
  
class BlockHandler(object):
  def __init__(self, block_record, address_manager, context, policy=None):
    self.id = block_record["id"]
    self.name = block_record["name"]
    self.args = block_record["args"]
    self.context = context
    self.address_manager = address_manager
    self.ipaddress = address_manager.get_ipaddress(block_record["at"] if block_record.has_key("at") else None)
    self.master_port = address_manager.get_master_port()
    self.policy = policy
    self.version = block_record["version"] if block_record.has_key("version") \
                                   else naming.DEFAULT_VERSION
    if using_engage:
      resource_key = naming.get_block_resource_key(self.name,
                                                   self.version)
      datablox_engage_adapter.install.install_block(resource_key)

    self.connections = {}
    self.ports = {}
    self.last_load = 0
    self.timeouts = 0
    path = naming.block_path(bloxpath, self.name, self.version)
    if not os.path.isfile(path):
      logger.error("Could not find the block " + path)
      raise NameError
  
  #creates an output port if it does not exist
  def get_output_port_connections(self, from_port):
    return get_or_default(self.connections, from_port, ["output"])
  
  #creates an output port if it does not exist
  def get_input_port_connections(self, to_port):
    return get_or_default(self.connections, to_port, ["input"])
  
  #a basic block has the same ipaddress for every port
  #but a block-group may have different ipaddresses for individual ports
  def get_ipaddress(self, to_port):
    return self.ipaddress
    
  def create_basic_config(self):
    config = {}
    config["name"] = self.name
    config["id"] = self.id
    config["args"] = self.args
    config["log_level"] = log_level
    config["master_port"] = get_url(self.ipaddress, self.master_port)
    config["ports"] = self.connections
    if self.policy != None:
      config["policy"] = self.policy
    return config
  
  def add_additional_config(self, config):
    #used by join and shard blocks
    pass

  def start(self):
    logger.info("starting %s" % self.name)
    config = self.create_basic_config()
    self.add_additional_config(config)
    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("ADD NODE", config))
    socket.connect(get_url(self.ipaddress, 5000))
    socket.send(message)
    logger.info("waiting for caretaker to load " + self.name)
    res = json.loads(socket.recv())
    socket.close()
    if not res:
      logger.error("Could not start block " + self.name)
      raise NameError
    else:
      logger.info(self.name + " loaded")
    return self.sync()

  def stop(self):
    pass
    
  def sync(self):
    url = get_url(self.ipaddress, self.master_port)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(url)
    logger.info("syncing with block %s at url %s" % (self.name, url))
    syncclient.send('')
    # wait for synchronization reply
    # TODO: hardcoded wait for 8 seconds
    res = timed_recv(syncclient, 8000)
    syncclient.close()
    return False if res == None else True
  
  def connect_to(self, from_port, to_block, to_port, connection_url=None):
    if connection_url == None:
      connection_port_num = self.address_manager.new_port()
      connection_url = get_url(to_block.get_ipaddress(to_port), connection_port_num)
    from_connections = self.get_output_port_connections(from_port)
    from_connections.append(connection_url)
    to_connections = to_block.get_input_port_connections(to_port)
    if len(to_connections) > 1:
      try:
        #it's ok for join url to have multiple inputs
        to_connections.index(connection_url)
        pass
      except ValueError:
        logger.error("Cannot add multiple input connections")
        raise NameError
    else:
      to_connections.append(connection_url)

  def poll_load(self):
    port = self.master_port
    message = json.dumps(("POLL", {}))
    socket = self.context.socket(zmq.REQ)
    socket.connect(get_url(self.ipaddress, port))
    socket.send(message)
    #wait for 4 sec
    load = timed_recv(socket, 4000)
    socket.close()
    if load != None:
      self.timeouts = 0
      load = json.loads(load)
      logger.info("%s has served %r (%r)" % (self.id, load, (load - self.last_load)))
      self.last_load = load
      #shut down
      if load == -1:
        logger.info("%s has shutdown" % (self.id))
        return []
      #block timed out
      else:
        return [load]
    else:
      logger.info("** Master: %s timed out" % self.id)
      self.timeouts += 1
      if self.timeouts > 3:
        return None
      else:
        return [0]

class RPCHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    self.id = block_record["id"]
    self.name = block_record["name"]
    self.args = block_record["args"]
    self.context = context
    self.address_manager = address_manager
    #TODO: we don't really need it, but put master node's ip in it
    self.ipaddress = "127.0.0.1"
    self.master_port = address_manager.get_master_port()
    self.policy = policy
    self.version = block_record["version"] if block_record.has_key("version") \
                                   else naming.DEFAULT_VERSION
    self.connections = {}
    self.ports = {}
    self.last_load = 0
    self.timeouts = 0
    self.webserver_process = None
  
  def start(self):
    connections_file_name = "connections"
    with open(connections_file_name, 'w') as f:
      f.write(json.dumps(self.connections))
      
    connections_file_path = os.path.join(os.getcwd(), connections_file_name)
    webserver_script = os.path.join(os.path.dirname(__file__),
                                     "webservice.py")
    command = [sys.executable, webserver_script, connections_file_path]
    self.webserver_process = subprocess.Popen(command)
    return True
    
  def stop(self):
    if self.webserver_process:
      self.webserver_process.terminate()

  def poll_load(self):
    if self.webserver_process.poll() == None:
      logger.info("RPC block is working")
      return [0]
    else:
      logger.info("RPC block has shutdown")
      return []
  
class DynamicJoinHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    BlockHandler.__init__(self, block_record, address_manager, context, policy)
    self.subscribers = 0
    self.join_port = address_manager.new_port()
  
  def join_url(self):
    return get_url(self.ipaddress, self.join_port)
    
  def add_subscriber(self):
    self.subscribers += 1
  
  #called before start by superclass BlockHandler
  def add_additional_config(self, config):
    config["subscribers"] = self.subscribers  
  
class ShardHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    BlockHandler.__init__(self, block_record, address_manager, context, policy)
    block_class = get_block_class(self.name)
    self.initial_configs = block_class.initial_configs(self.args)
    self.node_type = block_class.node_type()
    self.block_handlers = []
    self.join_handler = None
    self.populate()
    #sometimes the shard could shutdown before its blocks do
    self.shard_shutdown = False

  def block_id(self, block_num):
    return self.id + "-element-" + str(block_num)

  def join_id(self):
    return self.id + "-join"
    
  def populate(self):
    block_configs = self.initial_configs
    assert(len(block_configs) > 0)
    block_type = self.node_type
    block_name = block_type["name"]
    input_port = block_type["input_port"]
    logger.info("num blocks in shard %d" % (len(block_configs)))
    if block_type.has_key("output_port"):
      #optimization: creating the join block on the same node as the shard - TODO: verify this
      join_record = {}
      join_record["id"] = self.join_id()
      join_record["name"] = "dynamic-join"
      join_record["args"] = {}
      join_record["at"] = self.ipaddress
      self.join_handler = DynamicJoinHandler(join_record, self.address_manager, self.context, self.policy)

    for i, block_config in enumerate(block_configs):
      output_port = "output"+str(i)
      rec = {"id": self.block_id(i), "name": block_name, "args": block_config}
      block_handler = create_handler(rec, self.address_manager, self.context, self.policy)
      self.block_handlers.append(block_handler)
      #hack, this will get substituted to the right port in get_output_port_connections
      self.connect_to("<<" + output_port, block_handler, input_port)
      if block_type.has_key("output_port"):
        #TODO: remove hardcoded join input port name
        block_handler.connect_to(block_type["output_port"], self.join_handler, "input", self.join_handler.join_url())
        self.join_handler.add_subscriber()

  # give join node's output port
  def get_output_port_connections(self, from_port):
    #hack to deal with connecting the shard to join node
    if from_port.find("<<") != -1:
      return BlockHandler.get_output_port_connections(self, from_port[2:])
    else:
      return self.join_handler.get_output_port_connections("output")
    
  #called before start by superclass BlockHandler
  def add_additional_config(self, config):
    config["num_blocks"] = len(self.initial_configs)

  def start(self):
    res = True
    for bh in self.block_handlers:
      res = res and bh.start()
    if self.join_handler:
      res = res and self.join_handler.start()
    res = res and BlockHandler.start(self)
    return res
  
  def stop(self):
    for bh in self.block_handlers:
      bh.stop()
    BlockHandler.stop(self)
  
  def poll_load(self):
    loads = []
    running_blocks = []
    for bh in self.block_handlers:
      load = bh.poll_load()
      if load != None and load != []:
        running_blocks.append(bh)
        loads.extend(load)
    if self.join_handler:
      load = self.join_handler.poll_load()
      if load != None and load != []:
        loads.extend(load)
    if not self.shard_shutdown:
      load = BlockHandler.poll_load(self)
      if load != None:
        if load == []:
          self.shard_shutdown = True
        loads.extend(load)
    self.block_handlers = running_blocks
    return loads
    
class GroupHandler(BlockHandler):
  #group_record is the specification of the group (like a "class")
  #block_record is the instance of the group (like an "object")
  def __init__(self, block_record, group_record, address_manager, context):
    self.id = block_record["id"]
    self.name = group_record["group-name"]
    self.address_manager = address_manager
    self.context = context
    self.group_ports = group_record["group-ports"] if group_record.has_key("group-ports") else {}
    self.policies = group_record["policies"] if group_record.has_key("policies") else {}
    self.group_args = block_record["args"]
    #substitute group-args to hold proper arguments
    block_records = copy.deepcopy(group_record["blocks"])
    for b in block_records:
      if b["args"] == "group-args":
        b["args"] = self.group_args
    self.block_handlers = [create_handler(b, self.address_manager, self.context, self.policies.get(b["id"])) 
                            for b in block_records]
    self.block_hash = {}
    for bh in self.block_handlers:
      self.block_hash[bh.id] = bh
    self.set_initial_connections(group_record["connections"])
  
  def get_output_port_connections(self, from_port):
    try:
      block_name, port = self.group_ports[from_port]
      handler = self.block_hash[block_name]
      return handler.get_output_port_connections(from_port)
    except KeyError:
      logger.error("Block-group %s does not have a mapping for port %s" % (self.name, from_port))
      raise NameError

  def get_input_port_connections(self, to_port):
    try:
      block_name, port = self.group_ports[to_port]
      handler = self.block_hash[block_name]
      return handler.get_input_port_connections(to_port)
    except KeyError:
      logger.error("Block-group %s does not have a mapping for port %s" % (self.name, from_port))
      raise NameError
  
  def get_ipaddress(self, port):
    try:
      block_name, port = self.group_ports[port]
      handler = self.block_hash[block_name]
      return handler.get_ipaddress(port)
    except KeyError:
      logger.error("Block-group %s does not have a mapping for port %s" % (self.name, from_port))
      raise NameError
    
  def set_initial_connections(self, connections):
    for f, t in connections:
      (from_id, from_port) = get_single_item(f)
      (to_id, to_port)  = get_single_item(t)
      from_block = self.block_hash[from_id]
      # #if we have a shard, connect the join node instead
      # #TODO: hardcoded join output port name
      # if from_block.has_key("join_node"):
      #   from_block = from_block["join_node"]
      #   from_port = "output"
      to_block = self.block_hash[to_id]
      from_block.connect_to(from_port, to_block, to_port)
  
  def start(self):
    res = True
    for bh in self.block_hash.values():
      res = res and bh.start()
    return res
  
  def stop(self):
    for bh in self.block_hash.values():
      bh.stop()

  def poll_load(self):
    return self.poll_all_loads()
  
  def poll_all_loads(self):
    #it's initially None so that it will stay None if all the blocks in the group timeout
    #and we will return None in that case
    all_loads = None
    for bh in self.block_hash.values():
      loads = bh.poll_load()
      if loads != None:
        #shutdown
        if loads == []:
          self.block_hash.pop(bh.id)
        if all_loads == None:
          all_loads = []
        all_loads.extend(loads)
    return all_loads
    
class AddressManager(object):
  def __init__(self, ipaddress_list):
    self.master_port = 6500
    self.ipaddress_hash = {}
    self.port_num_gen = PortNumberGenerator()
    for ip in ipaddress_list:
      self.ipaddress_hash[ip] = 0
  
  def get_master_port(self):
    self.master_port += 2
    return self.master_port

  def get_all_ip_addresses(self):
    return self.ip_address_hash.keys()
  
  #if selected_ipaddress is None, return a new ipaddress
  #otherwise if selected_ipaddress exists in the list, return that otherwise raise error
  def get_ipaddress(self, selected_ipaddress):
    if selected_ipaddress == None:
      return self.select_ipaddress()
    elif self.ipaddress_hash.has_key(selected_ipaddress):
      return selected_ipaddress
    else:
      logger.error("No ipaddress ", selected_ipaddress)
      raise NameError
  
  def select_ipaddress(self):
    #select the node which has the least number of running blocks
    ips = self.ipaddress_hash.items()
    min_ip = (ips[0][0], ips[0][1])
    for k, v in ips:
      if min_ip[1] > v:
        min_ip = (k, v)
    #increment the blocks on this one
    self.ipaddress_hash[min_ip[0]] = min_ip[1] + 1
    return min_ip[0]
  
  def new_port(self):
    return self.port_num_gen.new_port()

class DjmAddressManager(AddressManager):
  """When running with Engage and the Distributed Job Manager,
  we delegate the management of nodes to the DJM.
  """
  def __init__(self, djm_job):
    AddressManager.__init__(self,
                            [node["name"] for node in djm_job.nodes])
    self.djm_job = djm_job

  def select_ipaddress(self):
    node_name = AddressManager.select_ipaddress(self)
    return (self.djm_job.get_node(node_name))["datablox_ip_address"]
  
  def get_ipaddress(self, selected_ipaddress):
    """When running under engage, the selected_ipaddress is actually
    the name of the node. We map that to the ip address of the node.
    """
    if selected_ipaddress == None:
      return self.select_ipaddress()
    elif self.djm_job.has_node(selected_ipaddress):
      return (self.djm_job.get_node(selected_ipaddress))["datablox_ip_address"]
    else:
      logger.error("No node with name %s" % selected_ipaddress)
      raise NameError

  def get_all_ip_addresses(self):
    return [node["datablox_ip_address"] for node in self.djm_job.nodes]
    
class Master(object):
  def __init__(self, _bloxpath, config_file, ip_addr_list,
               _using_engage, _log_level=logging.INFO):
    global global_config, bloxpath, using_engage, log_level
    bloxpath = _bloxpath
    using_engage = _using_engage
    log_level = _log_level
    if using_engage:
      logger.info("Running with Engage deployment home at %s" % \
                  engage_file_locator.get_dh())
      djm_job = djm_server.start_job_and_get_nodes(ip_addr_list,
                                                   os.path.basename(config_file))
      self.address_manager = DjmAddressManager(djm_job)
    else:
      self.address_manager = AddressManager(ip_addr_list)

    add_blox_to_path(_bloxpath)
    self.context = zmq.Context()
    global_config = self.get_config(config_file)
    #old style, flat config
    #convert it into a one group list.
    if not (type(global_config) == list):
      global_config["group-name"] = "main"
      global_config = [global_config]
    #can fill this with command line args
    main_block_rec = {"id": "main_inst", "args": {}}
    self.main_block_handler = GroupHandler(main_block_rec, get_group("main"), self.address_manager, self.context)
    if not self.main_block_handler.start():
      self.stop_all("Master: Could not start all blocks. Ending the run")

    try:
      self.run()
    except Exception, e:
      logger.exception("Master run aborted due to exception %s" % e)
      if using_engage:
        self.address_manager.djm_job.stop_job(successful=False,
                                              msg="Master run stopped due to exception %s" % e)
      raise
    if using_engage:
      self.address_manager.djm_job.stop_job(successful=True)
    return
  
  def get_config(self, config_file):
    with open(config_file) as f:
      return json.load(f)

  def stop_all(self, msg):
    logger.error(msg)
    try:
      self.main_block_handler.stop()
      logger.info("Master: trying to stop all blocks")
      for ip in self.address_manager.get_all_ip_addresses():
        self.stop_all_node(ip)
      logger.info("done, quitting")
      self.context.term()
    finally:
      if using_engage:
        self.address_manager.djm_job.stop_job(successful=False,
                                              msg=msg)
    sys.exit(1)

  def stop_all_node(self, ipaddress):
    socket = self.context.socket(zmq.REQ)
    message = json.dumps(("STOP ALL", {}))
    socket.connect(get_url(ipaddress, 5000))
    socket.send(message)
    logger.info("waiting for caretaker at %s to stop all blocks " % ipaddress)
    res = json.loads(socket.recv())
    socket.close()

  def run(self):
    try:
      while True:
        res = self.main_block_handler.poll_load()
        if len(res) == 0:
          logger.info("Master: no more running nodes, quitting")
          return
        #todo: hard coded 10
        time.sleep(10)
    except KeyboardInterrupt:
      self.stop_all("Got a keyboard interrupt") # does not return
      
