import zmq
import json
import time
import copy
import subprocess
import logging
import csv

import naming
from block import *
from shard import *

import engage_utils.text_tables as text_tables

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

# The timeout for polling a remote block, in milliseconds.
# See issue #62 for details.
POLL_TIMEOUT_MS = 10000

def id_wo_prefix(block_id):
  """Shorten a block id by removing the main_inst. prefix if present.
  """
  return block_id[len('main_inst.'):] if block_id.startswith('main_inst.') \
         else block_id


class ResourceManager(object):
  def __init__(self):
    # requests made from src to dest, except for rows with the same src/dest index,
    # which are used for requests served.
    self.block_loads = {}
    self.block_times = {}
    #this keeps track of timed out and shutdown blocks
    self.block_status = {}
    self.load_history = defaultdict(list)
    self.shards = {}

  def initialize_shard(self, id, name):
    """ASK_SAI: Not sure what this is for -- the shard structure is initialized, but never used!
    """
    self.shards[id] = name
    
  def poll_completed(self):
    print "Shards that can be parallelized:", self.shards

  def initialize_block_stats(self, block_id):
    self.block_loads[block_id] = {}
    self.block_status[block_id] = BlockStatus.STARTUP
    self.block_times[block_id] = 0

  def is_block_running(self, block_id):
    status = self.block_status[block_id]
    return status in BlockStatus.alive_status_values

  def any_blocks_running(self):
    for status in self.block_status:
      if status in BlockStatus.alive_status_values:
        return True
    return False

  def set_block_status(self, block_id, status):
    self.block_status[block_id] = status
    
  def update_requests_made(self, src_block, dest_block, request_cnt):
    """Called by block handler"""
    self.block_loads[src_block][dest_block] = request_cnt

  def update_requests_served(self, block_id, total_served):
    """Called by block handler. We store the requests served in the otherwise
    unused self-request entry"""
    self.block_loads[block_id][block_id] = -1 * total_served

  def update_total_times(self, block_id, processing_time, poll_time):
    """Called by block handler
    """
    self.block_times[block_id] = processing_time

  def has_timeouts(self):
    """Return True if there are are any blocks in the TIMEOUT
    or DEAD state
    """
    for i, v in self.block_status.items():
      if v == BlockStatus.TIMEOUT:
        logger.info("%s has a timeout" % i)
        return True
      elif v == BlockStatus.DEAD:
        logger.info("%s crashed" % i)
        return True
    return False
    
  def write_loads(self, poll_start_time):
    # print "Block loads", self.block_loads
    loads = defaultdict(int)
    for poller_id, d in self.block_loads.items():
      #this block has timed out, but add an entry anyway
      #to keep it in sync with other block loads
      if d == {}:
        loads[poller_id] += 0
      else:
        for block_id, load in d.items():
          # ASK_SAI: Should we skip the case where poller_id == block_id,
          # since that is used for requests served?
          loads[block_id] += load
    time_per_req = {}
    for i, t in self.block_times.items():
      try:
        time_per_req[i] = t/(-1 * self.block_loads[i][i])
      except (KeyError, ZeroDivisionError):
        time_per_req[i] = 0
    etas = [(l * time_per_req[i], i) for i, l in loads.items()]
    etas.sort()
    print "ETAs"
    for e in etas:
      print "%r -> %.3f (%.3f x %r)" % (e[1], e[0], time_per_req[e[1]], loads[e[1]])
      self.load_history[e[1]].append(e[0])
    duration = time.time() - poll_start_time
    #writing the time stamps of the polls in the same dictionary
    self.load_history["times"].append(duration)
    # with open("loads.json", 'w') as f:
    #   json.dump(dict([(e[1], e[0]) for e in etas]), f)

  def write_final_perfstats(self, loads_csv_file=None):
    logger.debug("Message counts:")
    for (s, dct) in self.block_loads.items():
      for (d, cnt) in dct.items():
        if s != d:
          logger.info("  %s => %s: %d msgs" % (s, d, cnt))
    if loads_csv_file==None:
      return
    with open(loads_csv_file, 'wb') as f:
      w = csv.writer(f)
      times = self.load_history["times"]
      del(self.load_history["times"])
      block_ids = self.load_history.keys()
      loads = self.load_history.values()
      #write the legend
      w.writerow(["Time"] + block_ids)
      for i in range(len(times)):
        row = [times[i]] + [v[i] for v in loads]
        w.writerow(row)

class BlockPerfStats(object):
  def __init__(self, block_id):
    self.block_id = block_id
    self.status = BlockStatus.STARTUP
    self.total_requests_served = 0
    self.period_requests_served = 0
    # requests made from other blocks to this one,
    # indexed by source block id
    self.requests_made_by_block = {}
    self.total_requests_made = 0 # sum of all requests by block
    self.total_processing_time = 0.0
    self.period_processing_time = 0.0
    self.total_poll_time = 0.0
    self.period_poll_time = 0.0
    self.average_loads = []
    self.last_average_load = 0

  def update_requests_served(self, total_served):
    self.period_requests_served = total_served - self.total_requests_served
    self.total_requests_served = total_served

  def update_requests_made(self, src_block, cnt):
    """The count is the running total from the source
    block to this block. We update the total for the
    source and then adjust the global total.
    """
    if not self.requests_made_by_block.has_key(src_block):
      self.requests_made_by_block[src_block] = 0
    old_reqs = self.requests_made_by_block[src_block]
    self.requests_made_by_block[src_block] = cnt
    self.total_requests_made += cnt - old_reqs

  def update_total_times(self, processing_time, poll_time):
    self.period_processing_time = processing_time - self.total_processing_time
    self.total_processing_time = processing_time
    self.period_poll_time = poll_time - self.total_poll_time
    self.total_poll_time = poll_time

  def add_load(self, duration):
    if self.status==BlockStatus.ALIVE:
      ## load = float(self.period_processing_time) / duration
      period_time = self.period_processing_time + self.period_poll_time
      load = self.period_processing_time/period_time if period_time>0.0 else 0.0
      self.average_loads.append(load)
      self.last_average_load = load
    elif self.status==BlockStatus.BLOCKED:
      self.average_loads.append(1.0)
      self.last_average_load = 1.0
    else:
      self.average_loads.append(-1.0)
      self.last_average_load = -1.0

  def queue_size(self):
    return self.total_requests_made - self.total_requests_served \
           if self.status in BlockStatus.alive_status_values \
           else -1
  
  def time_per_req(self):
    return 1000.0*self.total_processing_time/float(self.total_requests_served) \
           if self.total_requests_served>0 else 0

  def average_load(self, duration):
    """Avg load over the course of the run"""
    return self.total_processing_time / duration if duration > 0.0 else 0.0

class LoadBasedResourceManager(object):
  """An alternative to the ResourceManager class
  which just estimates the load level for each block
  over each timeframe.
  """
  def __init__(self):
    self.block_stats = {} # indexed by block id
    self.num_polls = 0
    self.poll_times = []
    self.stats_table = text_tables.Table([
                         text_tables.RightPaddedCol('Block', 20),
                         text_tables.RightPaddedCol('Status', 7),
                         text_tables.AltValueCol(text_tables.PctCol('Load', 0),
                                                 lambda v: v >= 0.0, 'N/A'),
                         text_tables.IntCol('Recent Reqs', 5),
                         text_tables.IntCol('Total Reqs', 9),
                         text_tables.AltValueCol(text_tables.FloatCol('Time/req (ms)', 5, 1),
                                                 lambda v: v != 0, 'N/A'),
                         text_tables.AltValueCol(text_tables.IntCol('Queue Size',
                                                                    8),
                                                 lambda v: v >= 0, 'N/A')
                       ])
    self.first_poll_start_time = -1

  def initialize_shard(self, id, name):
    pass # Don't care

  def poll_completed(self):
    pass # Don't care

  def initialize_block_stats(self, block_id):
    self.block_stats[block_id] = BlockPerfStats(block_id)

  def is_block_running(self, block_id):
    assert self.block_stats.has_key(block_id)
    status = self.block_stats[block_id].status
    return status in BlockStatus.alive_status_values

  def any_blocks_running(self):
    for stats in self.block_stats.values():
      if stats.status in BlockStatus.alive_status_values:
        return True
    return False

  def set_block_status(self, block_id, status):
    assert self.block_stats.has_key(block_id)
    self.block_stats[block_id].status = status

  def update_requests_made(self, src_block, dest_block, request_cnt):
    assert self.block_stats.has_key(dest_block)
    self.block_stats[dest_block].update_requests_made(src_block, request_cnt)

  def update_requests_served(self, block_id, total_served):
    assert self.block_stats.has_key(block_id)
    self.block_stats[block_id].update_requests_served(total_served)

  def update_total_times(self, block_id, processing_time, poll_time):
    assert self.block_stats.has_key(block_id)
    self.block_stats[block_id].update_total_times(processing_time,
                                                  poll_time)

  def has_timeouts(self):
    """Return True if there are are any blocks in the TIMEOUT
    or DEAD state
    """
    for stats in self.block_stats.values():
      if stats.status==BlockStatus.TIMEOUT:
        logger.info("%s has a timeout" % stats.block_id)
        return True
      elif stats.status == BlockStatus.DEAD:
        logger.info("%s crashed" % stats.block_id)
        return True
    return False

  def write_loads(self, poll_start_time):
    """Print out the current load data and update historical data.
    """
    current_time = time.time()
    duration = current_time - poll_start_time
    if self.first_poll_start_time==(-1):
      self.first_poll_start_time = poll_start_time
    self.num_polls += 1
    self.poll_times.append(int(round(current_time)))
    self.stats_table.clear_rows()
    for stats in self.block_stats.values():
      stats.add_load(duration)
      self.stats_table.add_row([id_wo_prefix(stats.block_id),
                                stats.status,
                                stats.last_average_load,
                                stats.period_requests_served,
                                stats.total_requests_served,
                                stats.time_per_req(),
                                stats.queue_size()])
    self.stats_table.sort('Load', descending=True)
    self.stats_table.write_to_stream(sys.stdout)
                               
  def write_final_perfstats(self, loads_csv_file=None):
    duration = time.time() - self.first_poll_start_time
    logger.info("Final Statistics")
    logger.info("================")
    logger.info("Total duration: %.0f seconds" % duration)
    table = text_tables.Table([
              text_tables.RightPaddedCol('Block', 20),
              text_tables.AltValueCol(text_tables.PctCol('Load', 0),
                                      lambda v: v >= 0.0, 'N/A'),              
              text_tables.IntCol('Total Reqs', 9),
              text_tables.AltValueCol(text_tables.FloatCol('Time/req (ms)', 5, 1),
                                      lambda v: v != 0, 'N/A')
            ])
    for stats in self.block_stats.values():
      table.add_row([id_wo_prefix(stats.block_id),
                     stats.average_load(duration),
                     stats.total_requests_served,
                     stats.time_per_req()])
    table.sort('Load', descending=True)
    table.write_to_log(logger)
    block_ids = [stats.block_id for stats in self.block_stats.values()]
    block_ids.sort()
    if loads_csv_file==None:
      return
    with open(loads_csv_file, 'wb') as f:
      w = csv.writer(f)
      w.writerow(["Time"] + [id_wo_prefix(block_id) for block_id in block_ids])
      for i in range(self.num_polls):
        w.writerow([self.poll_times[i]-self.poll_times[0],] +
                   [round(self.block_stats[block_id].average_loads[i], 3) for block_id in block_ids])
    logger.info("Wrote load history to %s" % loads_csv_file)


#resource_manager = ResourceManager()
resource_manager = LoadBasedResourceManager()

BLOCK_SYNC_TIMEOUT_MS = 20000

# time between polls of the caretakers. Can override via a command line parameter
DEFAULT_POLL_INTERVAL = 4

# Number of times that a poll should result in a timeout before we consider
# the associated block as dead.
NUM_TIMEOUTS_BEFORE_DEAD = 15


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
debug_block_list = None
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
      resource_manager.initialize_shard(block_record["id"], block_record["name"])
      return ShardHandler(block_record, address_manager, context, policy)
    else:
      return BlockHandler(block_record, address_manager, context, policy)
  else:
    return GroupHandler(block_record, group, address_manager, context)

class Connection(object):
  #targets = [(block, port)]
  def __init__(self, parent, port, connection_type):
    self.parent = parent
    self.port = port
    self.connection_type = connection_type
    self.connection_urls = []
    self.targets = []
  
  def __repr__(self):
    return "connection_type " + self.connection_type + " connection_urls " + str(self.connection_urls)
    
class BlockHandler(object):
  def __init__(self, block_record, address_manager, context, policy=None):
    global resource_manager
    self.id = block_record["id"]
    self.name = block_record["name"]
    self.args = block_record["args"]
    self.context = context
    self.address_manager = address_manager
    self.ip_address = address_manager.get_ipaddress(block_record.get("at"))
    self.master_port = address_manager.get_master_port()
    self.policy = policy
    self.version = block_record["version"] if block_record.has_key("version") \
                                   else naming.DEFAULT_VERSION
    self.connections = {}
    self.ports = {}
    self.last_load = 0
    self.timeouts = 0
    self.last_poll_time = 0
    resource_manager.initialize_block_stats(self.id)
  
  #creates an output port if it does not exist
  def get_output_port_connections(self, from_port):
    c = self.connections.get(from_port)
    if c is None:
      c = Connection(self, from_port, "output")
      self.connections[from_port] = c 
    return c
  
  #creates an output port if it does not exist
  def get_input_port_connections(self, to_port):
    c = self.connections.get(to_port)
    if c is None:
      c = Connection(self, to_port, "input")
      self.connections[to_port] = c
    return c
  
  #a basic block has the same ipaddress for every port
  #but a block-group may have different ipaddresses for individual ports
  def get_ipaddress(self, to_port):
    return self.ip_address
    
  def create_basic_config(self):
    config = {}
    config["name"] = self.name
    config["id"] = self.id
    config["args"] = self.args
    short_id = id_wo_prefix(self.id)
    config["log_level"] = logging.DEBUG \
                            if (self.id in debug_block_list) or \
                               (short_id in debug_block_list) \
                            else log_level
    config["master_port"] = get_url(self.ip_address, self.master_port)
    config["ports"] = self.create_port_config()
    if self.policy != None:
      config["policy"] = self.policy
    config["ip_address"] = self.ip_address
    return config
  
  def add_additional_config(self, config):
    #used by join and shard blocks
    pass

  def create_port_config(self):
    config = {}
    for p,c in self.connections.items():
      config[p] = [c.connection_type] + c.connection_urls
    return config
    
  def start(self):
    logger.info("starting %s" % self.name)
    config = self.create_basic_config()
    self.add_additional_config(config)
    socket = self.context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    message = json.dumps(("ADD BLOCK", config))
    logger.debug("Connecting to %s" % get_url(self.ip_address, 5000))
    socket.connect(get_url(self.ip_address, 5000))
    socket.send(message)
    logger.info("waiting for caretaker to load %s (%s)" % (self.id, self.name))
    res = json.loads(socket.recv())
    logger.debug("caretaker response was %r" % res)
    socket.close()
    if not res:
      logger.error("Could not start block " + self.name)
      raise Exception("Could not start block " + self.name)
    else:
      logger.info(self.name + " loaded")
    return self.sync()

  def stop(self):
    pass
    
  def sync(self):
    url = get_url(self.ip_address, self.master_port)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(url)
    logger.info("syncing with block %s (%s) at url %s" % (self.id, self.name, url))
    syncclient.send('')
    # wait for synchronization reply
    # TODO: hardcoded wait for 20 seconds
    res = timed_recv(syncclient, BLOCK_SYNC_TIMEOUT_MS)
    syncclient.close()
    if res != None:
      resource_manager.set_block_status(self.id, BlockStatus.ALIVE)
      return True
    else:
      logger.error("Synchronization failed or timed out after %d ms" %
                   BLOCK_SYNC_TIMEOUT_MS)
      return False
  
  def connect_to(self, from_port, to_block, to_port, connection_url=None):
    if connection_url == None:
      connection_port_num = self.address_manager.new_port()
      connection_url = get_url(to_block.get_ipaddress(to_port), connection_port_num)

    from_connections = self.get_output_port_connections(from_port)
    from_connections.connection_urls.append(connection_url)
    to_connections = to_block.get_input_port_connections(to_port)
    from_connections.targets.append((to_connections.parent, to_connections.port))
    if len(to_connections.connection_urls) > 0:
      try:
        #it's ok for join url to have multiple inputs
        to_connections.connection_urls.index(connection_url)
        pass
      except ValueError:
        logger.error("Cannot add multiple input connections")
        raise Exception("Cannot add multiple input connections")
    else:
      to_connections.connection_urls.append(connection_url)
      to_connections.targets.append((self, from_port))
  
  def find_target(self, port):
    t = self.connections[port].targets
    assert(len(t)==1)
    return t[0]
    
  def update_load(self, loads):
    global resource_manager
    load = loads.get(self.id)
    #we should get a fresh entry
    if load != None and load[LoadTuple.LAST_POLL_TIME] != self.last_poll_time:
      self.timeouts = 0
      status, requests_made, requests_served, total_processing_time, total_poll_time, last_poll_time, pid = load
      self.last_poll_time = last_poll_time
      resource_manager.update_total_times(self.id, total_processing_time,
                                          total_poll_time)
      for p, r in requests_made.items():
        try:
          to_block, to_port = self.find_target(p)
          resource_manager.update_requests_made(self.id, to_block.id, r)
        except KeyError:
          print "Could not find port %s in block %s" % (p, self.id)
          raise
      total_served = sum(requests_served.values())
      self.last_load = total_served
      resource_manager.update_requests_served(self.id, total_served)
      if status == BlockStatus.ALIVE:
        resource_manager.set_block_status(self.id, BlockStatus.ALIVE)
      elif status == BlockStatus.BLOCKED:
        resource_manager.set_block_status(self.id, BlockStatus.BLOCKED)
      elif status == BlockStatus.STOPPED:
        logger.info("%s has shutdown" % (self.id))
        resource_manager.set_block_status(self.id, BlockStatus.STOPPED)
      elif status == BlockStatus.DEAD:
        logger.info("%s has crashed" % (self.id))
        resource_manager.set_block_status(self.id, BlockStatus.DEAD)
    else:
      logger.info("** Master: %s timed out" % self.id)
      self.timeouts += 1
      if self.timeouts > NUM_TIMEOUTS_BEFORE_DEAD:
        resource_manager.set_block_status(self.id, BlockStatus.TIMEOUT)

class RPCHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    global resource_manager
    BlockHandler.__init__(self, block_record, address_manager, context, policy)
    #TODO: we don't really need it, but put master node's ip in it
    self.ip_address = "127.0.0.1"
    self.webserver_process = None
    self.webserver_poll_file = "webservice_poll.json"
  
  def start(self):
    connections_file_name = "connections"
    with open(connections_file_name, 'w') as f:
      f.write(json.dumps(self.create_port_config()))
      
    connections_file_path = os.path.join(os.getcwd(), connections_file_name)
    webserver_script = os.path.join(os.path.dirname(__file__),
                                     "webservice.py")
    command = [sys.executable, webserver_script, connections_file_path]
    self.webserver_process = subprocess.Popen(command)
    return True
    
  def stop(self):
    if self.webserver_process:
      self.webserver_process.terminate()

  #the webservice does update its loads, but it only updates them on new requests
  def update_load(self, loads):
    alive = True
    if self.webserver_process.poll() == None:
      logger.info("RPC block is working")
    else:
      logger.info("RPC block has shutdown")
      alive = False
    try:
      with open(self.webserver_poll_file, 'r') as f:
        load = json.loads(f.read())
    except IOError:
      print "Could not find web service file"
      load = ["ALIVE", {}, {}, 0, 0, self.webserver_process.pid]
    if alive:
      load[0] = "ALIVE"
    else:
      load[0] = "SHUTDOWN"
    load[4] = time.time()
    loads[self.id] = load
    BlockHandler.update_load(self, loads)
  
class DynamicJoinHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    BlockHandler.__init__(self, block_record, address_manager, context, policy)
    self.subscribers = 0
    self.join_port = address_manager.new_port()
  
  def join_url(self):
    return get_url(self.ip_address, self.join_port)
    
  def add_subscriber(self):
    self.subscribers += 1
  
  #called before start by superclass BlockHandler
  def add_additional_config(self, config):
    config["subscribers"] = self.subscribers  
  
class ShardHandler(BlockHandler):
  def __init__(self, block_record, address_manager, context, policy=None):
    BlockHandler.__init__(self, block_record, address_manager, context, policy)
    if using_engage:
      # For shard blocks, we need to install the block, even on the master.
      # This is because the block class is instantiated to call its
      # initial_configs() method.
      block_version = naming.DEFAULT_VERSION # TODO get this from the block metadata
      resource_key = naming.get_block_resource_key(self.name, block_version)
      logger.info("Using engage to install resource %s" % resource_key)
      datablox_engage_adapter.install.install_block(resource_key)
      logger.info("Install of %s and its dependencies successful" % \
                  resource_key)      
    block_class = get_block_class(self.name)
    self.node_type = self.args["node_type"]
    self.initial_configs = block_class.initial_configs(self.args)
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
      self.join_handler = DynamicJoinHandler(join_record, self.address_manager, self.context, self.policy)

    for i, block_config in enumerate(block_configs):
      output_port = "output"+str(i)
      loc = block_config["at"] if block_config.has_key("at") else None
      rec = {"id": self.block_id(i), "name": block_name, "args": block_config, "at": loc}
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
    config["port_type"] = self.node_type["port_type"]

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
      
  def update_load(self, loads):
    global resource_manager
    running_blocks = []
    for bh in self.block_handlers:
      bh.update_load(loads)
      if resource_manager.is_block_running(bh.id):
        running_blocks.append(bh)
    if self.join_handler:
      self.join_handler.update_load(loads)
    if not self.shard_shutdown:
      BlockHandler.update_load(self, loads)
      if not resource_manager.is_block_running(self.id):
        self.shard_shutdown = True
    self.block_handlers = running_blocks
    #the shard won't be shutdown unless all the blocks are done
    if self.block_handlers == [] and self.shard_shutdown:
      resource_manager.set_block_status(self.id, BlockStatus.STOPPED)
    else:
      resource_manager.set_block_status(self.id, BlockStatus.ALIVE)

    
class GroupHandler(BlockHandler):
  #group_record is the specification of the group (like a "class")
  #block_record is the instance of the group (like an "object")
  def __init__(self, block_record, group_record, address_manager, context):
    global resource_manager
    self.id = block_record["id"]
    self.name = group_record["group-name"]
    self.address_manager = address_manager
    self.context = context
    self.policies = group_record["policies"] if group_record.has_key("policies") else {}
    self.group_args = block_record["args"]
    resource_manager.initialize_block_stats(self.id)
    #substitute group-args to hold proper arguments
    block_records = copy.deepcopy(group_record["blocks"])
    for b in block_records:
      b["id"] = self.full_id(b["id"]) 
      if b["args"] == "group-args":
        b["args"] = self.group_args
    self.block_handlers = [create_handler(b, self.address_manager, self.context, self.policies.get(b["id"])) 
                            for b in block_records]
    self.block_hash = {}
    for bh in self.block_handlers:
      self.block_hash[bh.id] = bh
    
    self.group_ports = {}
    if group_record.has_key("group-ports"):
      for port, b in group_record["group-ports"].items():
        (block_id, b_port) = b
        self.group_ports[port] = [self.full_id(block_id), b_port]
    
    self.set_initial_connections(group_record["connections"])
  
  #return the full id of the block, prepended with group's id
  #this id should be unique across the topology
  def full_id(self, id):
    return self.id + "." + id
    
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
      from_id = self.full_id(from_id)
      to_id = self.full_id(to_id)
      from_block = self.block_hash[from_id]
      # #if we have a shard, connect the join node instead
      # #TODO: hardcoded join output port name
      # if from_block.has_key("join_node"):
      #   from_block = from_block["join_node"]
      #   from_port = "output"
      try:
        to_block = self.block_hash[to_id]
      except KeyError:
        raise Exception("Topology file references invalid target in connection: %s" % to_id)
      from_block.connect_to(from_port, to_block, to_port)
  
  def start(self):
    res = True
    for bh in self.block_hash.values():
      res = res and bh.start()
    return res
  
  def stop(self):
    for bh in self.block_hash.values():
      bh.stop()

  def update_load(self, loads):
    self.poll_all_loads(loads)
  
  def poll_all_loads(self, loads):
    global resource_manager
    resource_manager.set_block_status(self.id, BlockStatus.ALIVE)
    running = False
    for bh in self.block_hash.values():
      bh.update_load(loads)
      if resource_manager.is_block_running(bh.id):
        running = True
      else:
        self.block_hash.pop(bh.id)
    if not running:
      resource_manager.set_block_status(self.id, BlockStatus.STOPPED)
    
class AddressManager(object):
  def __init__(self, ipaddress_list):
    self.master_port = 6500
    self.ip_address_hash = {}
    self.port_num_gen = PortNumberGenerator()
    for ip in ipaddress_list:
      self.ip_address_hash[ip] = 0
  
  def get_master_port(self):
    self.master_port += 2
    return self.master_port

  def get_all_ipaddresses(self):
    return self.ip_address_hash.keys()
  
  #if selected_ipaddress is None, return a new ipaddress
  #otherwise if selected_ipaddress exists in the list, return that otherwise raise error
  def get_ipaddress(self, selected_ipaddress):
    if selected_ipaddress == None:
      return self.select_ipaddress()
    elif self.ip_address_hash.has_key(selected_ipaddress):
      return selected_ipaddress
    else:
      logger.error("No ipaddress ", selected_ipaddress)
      raise NameError
  
  def select_ipaddress(self):
    #select the node which has the least number of running blocks
    ips = self.ip_address_hash.items()
    min_ip = (ips[0][0], ips[0][1])
    for k, v in ips:
      if min_ip[1] > v:
        min_ip = (k, v)
    #increment the blocks on this one
    self.ip_address_hash[min_ip[0]] = min_ip[1] + 1
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
      raise Exception("No node with name %s" % selected_ipaddress)

  def get_all_ipaddresses(self):
    return [node["datablox_ip_address"] for node in self.djm_job.nodes]
    
class Master(object):
  def __init__(self, _bloxpath, config_file, ip_addr_list,
               _using_engage, _log_level=logging.INFO,
               _debug_block_list=[],
               reuse_existing_installs=True,
               poll_interval=DEFAULT_POLL_INTERVAL,
               block_args=None,
               loads_file=None):
    # Kind of yucky - using global variables for some key parameters
    global global_config, bloxpath, using_engage, log_level, debug_block_list
    bloxpath = _bloxpath
    using_engage = _using_engage
    log_level = _log_level
    debug_block_list = _debug_block_list
    self.poll_interval = poll_interval
    if using_engage:
      logger.info("Running with Engage deployment home at %s" % \
                  engage_file_locator.get_dh())
      djm_job = djm_server.start_job_and_get_nodes(ip_addr_list,
                                                   os.path.basename(config_file),
                                                   reuse_existing_installs=reuse_existing_installs)
      self.address_manager = DjmAddressManager(djm_job)
    else:
      self.address_manager = AddressManager(ip_addr_list)

    add_blox_to_path(_bloxpath)
    self.context = zmq.Context()
    global_config = self.get_config(config_file, block_args=block_args)
    #can fill this with command line args
    main_block_rec = {"id": "main_inst", "args": {}}
    self.main_block_handler = GroupHandler(main_block_rec, get_group("main"), self.address_manager, self.context)
    self.start_time = time.time()
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
    if loads_file and (not os.path.isabs(loads_file)) and using_engage:
      # if not an abolute path, stick the loads file in the log directory
      loads_file = os.path.join(engage_file_locator.get_log_directory(),
                                loads_file)
    resource_manager.write_final_perfstats(loads_file)
    if using_engage:
      self.address_manager.djm_job.stop_job(successful=True)
    return
  
  def get_config(self, config_file, block_args=None):
    with open(config_file) as f:
      cfg = json.load(f)
    #old style, flat config
    #convert it into a one group list.
    if not (type(cfg) == list):
      cfg["group-name"] = "main"
      cfg = [cfg]
    if block_args:
      # we go through the blocks in the config file and set any args that we find
      # in the passed-in args
      for group in cfg:
        assert group.has_key("group-name"), "Invalid topology file, missing group-name property"
        gn = group["group-name"]
        if not block_args.has_key(gn): continue # no overrides for this group
        assert group.has_key("blocks"), "Group %s missing blocks property" % gn
        arg_blocks = block_args[gn]
        for block in group["blocks"]:
          assert block.has_key("id"), "Block in group %s is missing id property" % gn
          bn = block["id"]
          if not arg_blocks.has_key(bn): continue
          args = block["args"]
          for (k, v) in arg_blocks[bn].items():
            logger.info("Overriding group '%s' block '%s' property '%s' to %s" %
                        (gn, bn, k, v))
            args[k] = v
    return cfg

  def send_all_nodes(self, message):
    results = []
    for ip in self.address_manager.get_all_ipaddresses():
      socket = self.context.socket(zmq.REQ)
      socket.setsockopt(zmq.LINGER, 0)
      socket.connect(get_url(ip, 5000))
      socket.send(message)
      res = json.loads(socket.recv())
      results.append(res)
      socket.close()
    return results
    
  def stop_all(self, msg):
    logger.error(msg)
    try:
      self.main_block_handler.stop()
      logger.info("Master: trying to stop all blocks")
      message = json.dumps(("STOP ALL", {}))
      self.send_all_nodes(message)
      logger.info("done, quitting")
      self.context.destroy()
    finally:
      if using_engage:
        self.address_manager.djm_job.stop_job(successful=False,
                                              msg=msg)
    sys.exit(1)

  def poll_all_nodes(self):
    #each node returns a dict with block id and load status
    #we want to merge all of them together into one dict
    #so collect all the items as pairs and then create a final dict
    load_items = []
    message = json.dumps(("POLL", {}))
    dicts = self.send_all_nodes(message)
    for d in dicts:
      load_items.extend(d.items())
    loads = dict(load_items)
    # print loads
    return loads
    
  def report_end(self):
    message = json.dumps(("END RUN", {}))
    self.send_all_nodes(message)
    
  def running(self):
    global resource_manager
    return resource_manager.any_blocks_running()

  def has_timeouts(self):
    global resource_manager
    return resource_manager.has_timeouts()
    
  def run(self):
    logger.info("Run started")
    try:
      while True:
        time.sleep(self.poll_interval)
        loads = self.poll_all_nodes()
        self.main_block_handler.update_load(loads)
        resource_manager.write_loads(self.start_time)
        if self.has_timeouts():
          logger.info("Master: topology has timeouts or crashes, killing all blocks and exiting")
          self.stop_all("Quitting topology due to remaining blocks timing out")
          return
        elif not self.running():
          logger.info("Master: no more running nodes, quitting")
          self.report_end()
          return
    except KeyboardInterrupt:
      self.stop_all("Got a keyboard interrupt") # does not return
      
