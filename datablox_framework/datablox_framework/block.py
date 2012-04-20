import zmq
import time
import json
import threading
from collections import defaultdict
import logging
import sys
import os
import os.path
import urllib
from urlparse import urlparse
import socket
from Crypto.Cipher import DES

from fileserver import file_server_keypath

class Log(object):
  def __init__(self):
    self.log = {}
  
  def set_log(self, log):
    self.log = log
  
  def __str__(self):
    return self.log.__str__()
    
  def append_field(self, key, values):
    self.log[key] = values
  
  def remove_field(self, key):
    del self.log[key]

  def append_row(self, row):
    """row is a dict"""
    #new log
    if self.log == {}:
      for k, v in row.items():
        self.log[k] = [v]
    else:
      #make sure we have the same columns
      assert(set(row.keys())==set(self.log.keys()))
      for k, v in row.items():
        self.log[k].append(v)
  
  def filtered_log(self, filter_func):
    nl = Log()
    for row in self.iter_flatten():
      if filter_func(row) == True:
        nl.append_row(row)
    return nl

  def iter_flatten(self):
    if self.log.keys() == []:
      yield {}
    else:
      #all field-lists should have the same length
      #so get the length of the first field
      count = len(self.log[self.log.keys()[0]])
      for i in range(count):
        nd = {}
        for k in self.log.keys():
          nd[k] = self.log[k][i]
        yield nd
  
  #can use zip for this, but zip doesn't return a generator object
  def iter_fields(self, *keys):
    values = [self.log[key] for key in keys]
    for i in range(0, len(values[0])):
      row = [value[i] for value in values]
      yield row
  
class Port(object):
      QUERY = 0
      PUSH = 1
      AGNOSTIC = 2
      MASTER = 3
      CONTROL = 4
      
      NAMED = 0
      UNNAMED = 1
      
      #port_urls and sockets will be added to this object by blocks
      def __init__(self, port_name, port_type, keys_type, keys):
        self.name = port_name
        self.port_type = port_type
        self.keys_type = keys_type
        #do some additional checks here - length(key) = 1 if port_type = named
        self.keys = keys
        self.end_point = None
        #holds requests processed for input ports
        #and requests sent out for output ports
        self.requests = 0
        self.request_start_time = 0
      
      def connect_to(self, block):
        self.end_point = block

class InvalidConnection(Exception):
  def __init__(self, fromc, toc):
    self.fromc = fromc
    self.toc = toc
    
  def __str__(self):
    return "Invalid connection from " + fromc.name + " to " + toc.name
    
class PortNumberGenerator(object):
  def __init__(self):
    #start with port 7002
    #6000 onwards is for listening to the master
    self.port_num = 7000
    self.lock = threading.Lock()
  
  #leave one port number for control
  def new_port(self):
    with self.lock:
      self.port_num += 2
      return self.port_num

node_ipaddress = None

class BlockUtils(object):
  @staticmethod
  def get_ipaddress():
    global node_ipaddress
    if node_ipaddress == None:
      ip = socket.gethostbyname(socket.gethostname())
      #that hasn't worked, using a method described here: http://zeth.net/post/355/
      if ip[:4] == "127.":
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('google.com', 0))
        ip = s.getsockname()[0]
      node_ipaddress = ip
      return ip
    else:
      return node_ipaddress
    
  @staticmethod
  def generate_url_for_path(path):
    path = path.encode('utf-8')
    with open(file_server_keypath, 'r') as f:
      deskey = f.read()
    obj = DES.new(deskey, DES.MODE_ECB)
    padding = ''
    for i in range(0 if len(path)%8 == 0 else 8 - (len(path)%8)):
      padding += '/'
    path = padding + path
    enc_path = obj.encrypt(path)
    url_path = urllib.quote(enc_path)
    return "http://" + BlockUtils.get_ipaddress() + ":4990/?key=" + url_path
  
  @staticmethod
  def fetch_local_file(enc_path):
    with open(file_server_keypath, 'r') as f:
      deskey = f.read()
    obj = DES.new(deskey, DES.MODE_ECB)
    path = obj.decrypt(enc_path)
    path = path.decode('utf-8')
    print "fetching local file at path", path
    with open(path, 'r') as f:
      return f.read()
    
  @staticmethod
  def fetch_file_at_url(url):
    p = urlparse(url)
    if p.hostname == BlockUtils.get_ipaddress():
      url_enc_path = p.query[len("key="):].encode('ascii')
      enc_path = urllib.unquote(url_enc_path)
      return BlockUtils.fetch_local_file(enc_path)
    else:
      opener = urllib.FancyURLopener({})
      f = opener.open(url)
      return f.read()
  
class Block(threading.Thread):
  def __init__(self, master_url):
    threading.Thread.__init__(self)
    # the following 4 fields will be initialized by load_block.start just
    # before the call to on_load()
    self.id = None
    self.log_level = logging.INFO
    self.logger = None
    self.queue_size = 0
    self.connection_type = Port.AGNOSTIC
    master_port = Port("master", Port.MASTER, Port.UNNAMED, [])
    master_port.port_url = master_url
    self.master_port = master_port
    self.ports = [master_port]
    self.output_ports = {}
    self.input_ports = {}
    self.context = None
    #we have a different poller for control requests because the regular poller might get
    #filled with requests from other blocks and these requests might get behind
    #this is to prioritize control requests
    self.control_poller = None
    self.poller = None
    self.alive = True
    self.task = None
    self.total_processing_time = 0
    self.buffer_limit = 50
    self.current_buffer_size = defaultdict(int)
    self.buffered_pushes = defaultdict(list)

  def set_queue_size(self, size):
    self.queue_size = size
    self.log(logging.INFO, "setting queue size to %r" % size)
    
  def run(self):
    try:
      self.context = zmq.Context()
      self.ready_ports()
      self.log(logging.INFO, "ports are ready")
      #TODO: adding all times of the initial task to master port, change this
      if self.input_ports.keys() == []:
        self.task = [self.do_task(), self.master_port, []]
      self.start_listening()
    except KeyboardInterrupt:
      self.log(logging.INFO, "Stopping thread")
  
  def ready_ports(self):
    self.ready_master_port()

    #serve all input ports    
    for p in self.input_ports.keys():
      self.ready_input_port(p)
    
    #connect to all servers of the output ports
    for p in self.output_ports.keys():
      self.ready_output_port(p)
 
    self.control_poller = zmq.Poller()
    #Listen to Master
    self.control_poller.register(self.master_port.socket, zmq.POLLIN)

    self.poller = zmq.Poller()
    #Listen to all inputs
    for p in self.input_ports:
      if p.socket == None:
        self.log(logging.DEBUG,
                 "has a port %s, url %d none" % (p.name, p.url))
        raise NameError
      self.poller.register(p.socket, zmq.POLLIN)
    
  def ready_master_port(self):
    self.bind_rep_port(self.master_port)
    self.log(logging.INFO, "waiting for master to respond")
    #wait for master to synchronize
    self.master_port.socket.recv()
    self.log(logging.INFO, "master's online, starting other ports")
    ready_message = json.dumps(("CTRL", "READY"))
    self.log_send("CTRL", ready_message, self.master_port)
    self.master_port.socket.send(ready_message)
  
  def ready_input_port(self, port):
    if port.port_type == Port.PUSH:
      self.bind_query_port(port)
    elif port.port_type == Port.QUERY:
      self.bind_rep_port(port)
    else:
      raise NameError
  
  def ready_output_port(self, port):
    if port.port_type == Port.PUSH:
      self.listen_push_port(port)
    elif port.port_type == Port.QUERY:
      self.listen_req_port(port)
    else:
      raise NameError
    
  def get_one(self, _list):
    assert(len(_list) == 1)
    return _list[0]

  #PUSH ports now have queue size as a parameter
  #default value is 0 - for infinite queue size
  #this is set in load_block    
  def bind_query_port(self, port):
    port.socket = self.context.socket(zmq.PULL)
    port.socket.setsockopt(zmq.HWM, self.queue_size)
    port.socket.bind(port.port_url)
  
  def bind_rep_port(self, port):
    port.socket = self.context.socket(zmq.REP)
    port.socket.bind(port.port_url)

  #PUSH ports now have queue size as a parameter
  #default value is 0 - for infinite queue size
  #this is set in load_block
  def listen_push_port(self, port):
    port.sockets = []
    for port_url in port.port_urls:
      socket = self.context.socket(zmq.PUSH)
      socket.setsockopt(zmq.HWM, self.queue_size)
      socket.connect(port_url)
      port.sockets.append(socket)
  
  def listen_req_port(self, port):
    port_url = self.get_one(port.port_urls)
    socket = self.context.socket(zmq.REQ)
    socket.connect(port_url)
    port.sockets = [socket]
  
  def start_listening(self):
    while self.alive:
      if self.task or self.input_ports.keys() == []:
        self.process_pending_task()
        
      socks = dict(self.control_poller.poll(5))
      if socks != None and socks != {}:
        ports_with_data = [p for p in self.input_ports if p.socket in socks and socks[p.socket] == zmq.POLLIN]
        control_ports = [p for p in ports_with_data if p.port_type == Port.CONTROL]
      
        #process master instructions if any
        if socks.has_key(self.master_port.socket) and socks[self.master_port.socket] == zmq.POLLIN:
          message = json.loads(self.master_port.socket.recv())
          (control, data) = message
          self.log_recv(control, message, self.master_port)
          self.process_master(control, data)
      
        #process control instructions
        for p in control_ports:
          message = p.socket.recv()
          (control, log) = json.loads(message)
          self.log_recv(control, message, p)
          assert(control == "CTRL")
          self.process_control(p, log)
      
      #no more pending tasks, now deal with data ports
      if self.task == None:
        socks = dict(self.poller.poll(5))
        if socks != None and socks != {}:
          ports_with_data = [p for p in self.input_ports if p.socket in socks and socks[p.socket] == zmq.POLLIN]
          push_ports = [p for p in ports_with_data if p.port_type == Port.PUSH]
          query_ports = [p for p in ports_with_data if p.port_type == Port.QUERY]
      
          for p in push_ports:
            message = p.socket.recv()
            try:
              (control, log) = json.loads(message)
            except:
              self.log(logging.ERROR, "JSON parse error for message '%s' from %s" % (message, p.name))
              raise
            self.log_recv(control, message, p)
            if control == "END":
              self.process_stop(p, log)
            elif control == "BUFFERED PUSH":
              self.process_buffered_push(p, log)
            else:
              self.process_push(p, log)
          for p in query_ports:
            message = p.socket.recv()
            (control, log) = json.loads(message)
            self.log_recv(control, message, p)
            if control == "END":
              self.process_stop(p, log)
            else:
              self.process_query(p, log)
  
  def get_load(self):
    requests_made = defaultdict(int)
    for p in self.output_ports:
      requests_made[p.name] = p.requests
    
    requests_served = defaultdict(int)
    for p in self.input_ports:
      requests_served[p.name] = p.requests

    return (requests_made, requests_served)
  
  def respond_poll(self):
    rm, rs = self.get_load()
    load = json.dumps(("ALIVE", rm, rs, self.total_processing_time))
    self.log_send("POLL", load, self.master_port)
    self.master_port.socket.send(load)
    
  def process_master(self, control, data):
    if control == "POLL":
      self.respond_poll()
    else:
      self.log(logging.WARN, " Warning ** could not understand master")
  
  def process_control(self, control_data):
    self.log(logging.ERROR, "Block object %s should not be getting a control message" % (self.id))
    raise NotImplementedError
    
  def process_push(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    port.request_start_time = time.time()
    res = self.recv_push(port.name, log)
    e = time.time()
    self.total_processing_time += (e - port.request_start_time)
    if res != None:
      self.task = [res, port, []]
      return self.task
  
  def process_buffered_push(self, port, logs):
    #print self.id + " got buffered push"
    for i,log in enumerate(logs):
      res = self.process_push(port, log)
      #task is not done yet, queue pending logs
      if res != None:
        self.task[2] = logs[i+1:]
        return
    
  def process_query(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    # print self.id + " got a query query for port " + port.name
    port.request_start_time = time.time()
    res = self.recv_query(port.name, log)
    e = time.time()
    self.total_processing_time += (e - port.request_start_time)
    if res != None:
      self.task = [res, port, []]
      return self.task
    else:
      port.requests += 1
  
  def no_incoming(self):
    for subscribers in self.input_ports.values():
      if subscribers != 0:
        return False
    return True
    
  def process_stop(self, port, stop_msg):
    #we could have multiple nodes sending to an input port
    #DynamicJoin for example
    (block_name, recv_port_name) = stop_msg
    self.log(logging.INFO,
             "(%s, %s) stopped on (%s, %s)" % (block_name, recv_port_name, self.id, port.name))
    self.input_ports[port] -= 1
    
    if self.no_incoming():
      self.on_shutdown()
      self.shutdown()
  
  def process_pending_task(self):
    # self.log(logging.INFO,
    #          "processing pending task %r" % (self.task))
    if self.input_ports.keys() == []:
      assert(self.task != None)
    task, port, logs = self.task
    try:
      port.request_start_time = time.time()
      task.next()
      e = time.time()
      self.total_processing_time += (e - port.request_start_time)
    except StopIteration:
      if self.input_ports.keys() == []:
        self.shutdown()
      else:
        self.task = None
        port.requests += 1
        if logs != []:
          self.process_buffered_push(port, logs)
    
  def send(self, control, message, port):
    message = (control, message)
    json_log = json.dumps(message)
    self.log_send(control, json_log, port)
    if hasattr(port, "sockets"):
      for socket in port.sockets:
        socket.send(str(json_log))
    else:
      self.log(logging.WARN,
               "No connections to port %s, ignoring send of %s message" %
               (port.name, control))
  
  def do_task(self):
    """This method is only called when the block has no input ports (e.g. is
    a data source).
    """
    raise NotImplementedError
    
  def on_shutdown(self):
    pass

  def shutdown(self):
    self.flush_ports()
    
    for p in self.output_ports.keys():
      self.send("END", (self.id, p.name), p)
    self.alive = False
    self.report_shutdown()
    self.close_all_ports()
    self.log(logging.INFO, " Has shutdown")
    sys.exit(0)

  #do NOT set linger=0 for sockets here, we might not have sent all the data to output blocks yet
  def close_all_ports(self):
    for p in self.ports:
      if hasattr(p, "socket"):
        p.socket.close() 
      if hasattr(p, "sockets"):
        for socket in p.sockets:
          socket.close()
    self.context.term()
    
  def report_shutdown(self):
    self.log(logging.INFO, " waiting for master to poll to report shutdown")
    while True:
      control_data = json.loads(self.master_port.socket.recv())
      control, data = control_data
      if control == "POLL":
        rm, rs = self.get_load()
        message = json.dumps(("SHUTDOWN", rm, rs, self.total_processing_time))
        self.log_send("''", message, self.master_port)
        self.master_port.socket.send(message)
        break
      elif control == "CAN ADD":
        message = json.dumps((False, {}))
        self.log_send("''", message, self.master_port)
        self.master_port.socket.send(message)
    
  def on_load(self, config):
    raise NotImplementedError
  
  def add_port(self, port_name, port_type, keys_type, keys):
    port = Port(port_name, port_type, keys_type, keys)
    port.end_point = self
    self.ports.append(port)
    return port
  
  def recv_push(self, full_port_name, log):
    raise NotImplementedError
  
  def recv_query(self, port, log):
    raise NotImplementedError

  def recv_query_result(self, port, log):
    raise NotImplementedError
  
  def find_port(self, port_name):
    port = None
    for p in self.ports:
      if p.name == port_name:
        port = p
    
    if port == None:
      print [p.name for p in self.ports]
      self.log(logging.ERROR,
               "could not find port with name: " + port_name)
      raise NameError
    
    return port

  def push(self, port_name, log):
    assert self.current_buffer_size[port_name] == 0, \
      "Attempt to do an unbuffered push on port '%s' that has buffered data" % \
      port_name
    port = self.find_port(port_name)
    port.requests += 1
    self.send("PUSH", log.log, port)
  
  def buffered_push(self, port_name, log):
    self.buffered_pushes[port_name].append(log.log)
    self.current_buffer_size[port_name] += 1
    if self.current_buffer_size[port_name] > self.buffer_limit:
      self.log(logging.DEBUG, "port %s is full (%d messages)" %
               (port_name, self.current_buffer_size[port_name]))
      self.flush_port(port_name)
  
  def flush_ports(self):
    self.log(logging.DEBUG, "flushing all ports")
    for port_name in self.buffered_pushes.keys():
      self.flush_port(port_name)
  
  def flush_port(self, port_name):
    self.log(logging.DEBUG, "flushing port: %s" % port_name)
    buffered_pushes = self.buffered_pushes[port_name]
    port = self.find_port(port_name)
    self.send("BUFFERED PUSH", buffered_pushes, port)
    port.requests += self.current_buffer_size[port_name]
    self.buffered_pushes[port_name] = []
    self.current_buffer_size[port_name] = 0
  
  #query is blocking for now
  def query(self, port_name, log):
    port = self.find_port(port_name)
    port.requests += 1
    self.send("QUERY", log.log, port)
    res = self.get_one(port.sockets).recv()
    log_data = json.loads(res)
    log = Log()
    log.set_log(log_data)
    self.log_recv("QUERY response", res, port)
    return log
  
  def return_query_res(self, port_name, log):
    port = self.find_port(port_name)
    log_data = json.dumps(log.log)
    self.log_send('return_query_res', log_data, port)
    port.socket.send(log_data)
    
  def get_input_port_url(self, input_port_name):
    input_port = self.find_port(input_port_name)
    return input_port.port_url

  def add_output_connection(self, output_port_name, connection_port_url):
    output_port = self.find_port(output_port_name)
    if output_port.port_type == Port.QUERY and self.output_ports.has_key(output_port):
      self.log(logging.ERROR," connecting to an already connected output QUERY port")
      raise NameError
    try:
      port_urls = getattr(output_port, "port_urls")
      output_port.port_urls.append(connection_port_url)
    except AttributeError:
        output_port.port_urls = [connection_port_url]
    self.output_ports[output_port] = 1
  
  def add_input_connection(self, input_port_name, connection_port_url):
    input_port = self.find_port(input_port_name)
    if self.input_ports.has_key(input_port):
      self.log(logging.ERROR, " connecting to an already connected input port")
      raise NameError
    input_port.port_url = connection_port_url
    self.input_ports[input_port] = 1

  def initialize_logging(self, log_directory=None):
    """This should be called by load_block.py after the block object has
    been constructed, but before on_load() is called.
    """
    self.logger = logging.getLogger(self.id)
    self.logger.setLevel(self.log_level)
    if log_directory:
      logfile = os.path.join(log_directory,
                             "%s_%d.log" % (self.id, os.getpid()))
      handler = logging.FileHandler(logfile, delay=True)
      handler.setLevel(self.log_level)
      sys.stdout.write("[%s] logging %s\n" % (self.id, logfile))
      sys.stdout.flush()
    else:
      handler = logging.StreamHandler(sys.__stdout__)
      handler.setLevel(self.log_level)
    handler.setFormatter(logging.Formatter("[%(asctime)s][" + self.id +
                                           "] %(message)s",
                                           "%H:%M:%S"))
    self.logger.addHandler(handler)
    
  def log(self, log_level, log_msg):
    """Blocks should call this to provide consistent logging. The printed log
    messages will include the block id, so there is no need to include
    that in the log_msg.

    This should not be called in the __init__() method, as logging is not
    initialized until just before on_load()
    """
    ## print "[" + self.id + "] " + log_msg
    ## sys.stdout.flush()
    self.logger.log(log_level, log_msg)

  def log_send(self, control, serialized_msg, port):
    if self.log_level>=logging.DEBUG: return
    if len(serialized_msg) < 60:
      self.log(logging.DEBUG,
               "sending message '%s' => %s" % (control,
                                               port.name))
    else:
      self.log(logging.DEBUG,
               "sending message type %s len %d => %s" % (control,
                                                         len(serialized_msg),
                                                         port.name))
      
  def log_recv(self, control, serialized_msg, port):
    if self.log_level>=logging.DEBUG: return
    if len(serialized_msg) < 60:
      self.log(logging.DEBUG,
               "received message %s => '%s'" % (port.name,
                                                serialized_msg))
    else:
      self.log(logging.DEBUG,
               "received message %s => type %s len %d" % (port.name,
                                                          control,
                                                          len(serialized_msg)))
    

from functools import wraps

def benchmark(func):
  """
  This is meant to be called from individual blocks
  """
  import time
  from logging import ERROR, WARN, INFO, DEBUG
  @wraps(func)
  def wrapper(*args, **kwargs):
    obj = args[0] #the block object calling this function
    if not hasattr(obj, "benchmark_dict"):
      obj.benchmark_dict = {}
    d = obj.benchmark_dict
    start = time.time()
    res = func(*args, **kwargs)
    duration = time.time() - start
    if d.has_key("total_duration"):
      d["total_duration"] += duration
      d["num_calls"] += 1
    else:
      d["total_duration"] = duration
      d["num_calls"] = 1
    return res
  return wrapper

def print_benchmarks(func):
  from logging import ERROR, WARN, INFO, DEBUG
  @wraps(func)
  def wrapper(*args, **kwargs):
    res = func(*args, **kwargs)
    obj = args[0]
    if not hasattr(obj, "benchmark_dict"):
      obj.log(INFO, "perf: never called %r" % (func.__name__))
    else:
      d = obj.benchmark_dict
      avg = d["total_duration"]/d["num_calls"] if d["num_calls"] != 0 else 0
      obj.log(INFO, "perf: %r: calls: %r, total duration: %r, avg: %r" 
                      % (func.__name__, d["num_calls"], d["total_duration"], avg))
    return res
  return wrapper
