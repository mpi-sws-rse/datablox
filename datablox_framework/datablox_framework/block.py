
# -*- py-indent-offset:2 -*-

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
from urlparse import urlparse, parse_qs
import socket
from Crypto.Cipher import DES
import base64
import re

from fileserver import file_server_keypath
logger = logging.getLogger(__name__)
# cache the key here so that we avoid having to read the keyfile for each file
FILE_SERVER_KEY=None

class Log(object):
  def __init__(self):
    self.log = {}
  
  def set_log(self, log):
    self.log = log
  
  def __str__(self):
    return "Log: " + self.log.__str__()
    
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

  def num_rows(self):
    if self.log=={}: return 0
    for k, v in self.log.items():
      return len(v)
    
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
successes = 0

def encrypt_path(path, key):
  """Given the specified filesystem path, encrypt it and put it in a form
  usable in a url query string. We use DES encryption and base64 encoding.
  """
  assert isinstance(path, unicode), \
      "Path %s is not unicode, this will cause problems" % path
  assert path[0] == u'/', "Path '%s' is not an absolute path" % path
  obj = DES.new(key, DES.MODE_ECB)
  # we convert the path to utf-8 before encrypting it
  path = path.encode('utf-8')
  # Need to pad to a multiple of 8. This is tricky, as length is dependent
  # on encoding. We use regular 8-bit characters, which are assumed to be valid
  # utf-8.
  padding = ''
  for i in range(0 if len(path)%8 == 0 else 8 - (len(path)%8)):
    padding += '/'
  path = padding + path
  enc_path = obj.encrypt(path)
  return base64.urlsafe_b64encode(enc_path)

_leading_slash_re = re.compile(u"^[/]+", re.UNICODE)

def decrypt_path(encoded_path, key):
  # The encoded_path is encrypted and base64 encoded. Base64 decoding
  # doesn't seem to work on unicode strings (which is what we get back when
  # parsing the path from the url). We encode as ascii first (the base64 encoding
  # only uses valid ascii characters).
  encoded_path = encoded_path.encode("ascii")
  encrypted_path = base64.urlsafe_b64decode(encoded_path)
  obj = DES.new(key, DES.MODE_ECB)
  path = unicode(obj.decrypt(encrypted_path), encoding="utf-8")
  return _leading_slash_re.sub(u"/", path) # removing the padding


class TooManyErrors(Exception):
  """This is an exception class to be used for blocks that may encounter errors. The user
  can set the config parameter max_error_pct to a value between 0 and 100. If the error
  rate exceeds that percentage, then the block should be aborted with this exception.
  """
  def __init__(self, block, num_errors, num_total_events):
    pct = float(num_errors)/float(num_total_events) * 100.0
    msg = "Block '%s' of type '%s' aborting due to too many errors: %d errors out of %d events (%.0f%%)" % \
        (block.id, block.block_name, num_errors, num_total_events, pct)
    Exception.__init__(self, msg)
    self.msg = msg
    self.block_id = block.id
    self.block_name = block.block_name
    self.num_errors = num_errors
    self.num_total_events = num_total_events

  def __str__(self):
    return self.msg

  def __repr__(self):
    return "TooManyErrors(block_id=%s, block_name=%s, num_errors=%d, total_events=%d)" % \
        (self.block_id, self.block_name, self.num_errors, self.num_total_events)


def check_if_error_threshold_reached(block, num_errors, num_total_events):
  """Utility function to see if the block execution should be aborted due to 
  the percentage of errors exceeding the threshold. The block must have a
  max_error_pct member. Thows TooManyErrors if the threshold is exceeded.
  """
  if num_total_events < 100: # we need a big enough sample size before doing the check
    return
  error_pct = float(num_errors)/float(num_total_events)
  if error_pct>(float(block.max_error_pct)/100.0):
    raise TooManyErrors(block, num_errors, num_total_events)

  
class URLOpenError(Exception):
  def __init(self, errcode, msg):
    Exception.__init__(self, msg)
    self.errcode = errcode

class ErrorCheckingURLopener(urllib.FancyURLopener):
  """We need to subclass the url opener from urllib because it silently ignores
  errors!!"""
  def __init__(self, *args, **kwargs):
    urllib.FancyURLopener.__init__(self, *args, **kwargs)
  def http_error_default(self, url, fp, errcode, errmsg, headers):
    raise URLOpenError(errcode, "Got error %d for url %s: '%s'" % (errcode, url, errmsg))

class BlockUtils(object):
  @staticmethod
  def get_ipaddress():
    global node_ipaddress
    if node_ipaddress == None:
      try:
        ip = socket.gethostbyname(socket.gethostname())
        #that hasn't worked, using a method described here: http://zeth.net/post/355/
        if ip[:4] == "127.":
          s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
          s.connect(('google.com', 0))
          ip = s.getsockname()[0]
      except socket.gaierror:
        ip = "127.0.0.1"
      if ip[:4] == "127.": logger.warn("Could only find local ip-address!")
      node_ipaddress = ip
      return ip
    else:
      return node_ipaddress
    
  @staticmethod
  def generate_url_for_path(path, block_ip=None, key_for_testing=None):
    try:
      statinfo = os.stat(path)
    except Exception, e:
      raise Exception("Unable to stat file at path %s: %s" %
                      (path, e))
    global FILE_SERVER_KEY
    if FILE_SERVER_KEY==None and key_for_testing==None:
      with open(file_server_keypath, 'r') as f:
        FILE_SERVER_KEY = f.read()
    url_path = encrypt_path(path,
                            key_for_testing if key_for_testing else FILE_SERVER_KEY)
    qs = urllib.urlencode([("len", statinfo.st_size), ("key", url_path)])
    if block_ip:
      ip = block_ip
    else:
      ip = BlockUtils.get_ipaddress()
    return "http://" + ip + ":4990/?" + qs
  
  @staticmethod
  def fetch_local_file(enc_path):
    global FILE_SERVER_KEY
    if not FILE_SERVER_KEY:
      with open(file_server_keypath, 'r') as f:
        FILE_SERVER_KEY = f.read()
    path = decrypt_path(enc_path, FILE_SERVER_KEY)
    # print "fetching local file at path", path
    with open(path, 'r') as f:
      return f.read()

  @staticmethod
  def get_url_or_decrypted_local_path(url, block_ip_address):
    """Parse the url and figure out if the fileserver is local.
    If so, decrypt the path and return it prefixed with file://.
    Otherwise, just return the original url. In both cases, the return value
    is a pair of the form (url/path, file_len).
    """
    p = urlparse(url)
    query_dict = parse_qs(p.query)
    assert query_dict.has_key("key"), "Url '%s' missing 'key' query parmameter" % url
    assert query_dict.has_key("len"), "Url '%s' missing 'len' query parmameter" % url
    expected_len = long(query_dict["len"][0])
    if (p.hostname == BlockUtils.get_ipaddress()) or \
       (p.hostname == block_ip_address):
      enc_path = query_dict["key"][0]
      global FILE_SERVER_KEY
      if not FILE_SERVER_KEY:
        with open(file_server_keypath, 'r') as f:
          FILE_SERVER_KEY = f.read()
      return ("file://" + decrypt_path(enc_path, FILE_SERVER_KEY), expected_len)
    else:
      return (url, expected_len)
    
  @staticmethod
  def fetch_file_at_url(url, block_ip_address, check_size=False):
    """Fetch a file from the fileserver at the specified url. Try to do it
    by a local read if possible. The block_ip_address parameter is used to help
    determine locality.

    If you pass in check_size as True, this function will return the data and
    the expected length (as obtained from the URL). Note that the expected length
    is not always correct - the file might have been changed since the last access.
    """
    global successes
    p = urlparse(url)
    query_dict = parse_qs(p.query)
    assert query_dict.has_key("key"), "Url '%s' missing 'key' query parmameter" % url
    assert query_dict.has_key("len"), "Url '%s' missing 'len' query parmameter" % url
    expected_len = long(query_dict["len"][0])
    if (p.hostname == BlockUtils.get_ipaddress()) or \
       (p.hostname == block_ip_address):
      key = query_dict["key"][0]
      data = BlockUtils.fetch_local_file(key)
    else:
      opener = ErrorCheckingURLopener({})
      f = opener.open(url)
      successes += 1
      if (successes % 50)==0:
        logger.info("Fetched %d files successfully" % successes)
      data = f.read()
    if check_size:
      return (data, expected_len)
    else:
      return data
  
class Block(threading.Thread):
  def __init__(self, master_url):
    threading.Thread.__init__(self)
    # the following 4 fields will be initialized by load_block.start just
    # before the call to on_load()
    self.id = None # The "id" field in metadata
    self.block_name = None # This is the name field in metadata, but that conflicts with the thread name
    self.log_level = logging.INFO
    self.logger = None
    self.queue_size = 0
    #poll file name will be set by load_block
    self.poll_file_name = None
    self.connection_type = Port.AGNOSTIC
    master_port = Port("master", Port.MASTER, Port.UNNAMED, [])
    master_port.port_url = master_url
    self.master_port = master_port
    self.ports = [master_port]
    self.output_ports = {}
    self.input_ports = {}
    self.pending_query_res_ports = {}
    self.context = None
    #we have a different poller for control requests because the regular poller might get
    #filled with requests from other blocks and these requests might get behind
    #this is to prioritize control requests
    self.control_poller = None
    self.poller = None
    self.alive = True
    self.task = None
    self.total_processing_time = 0
    self.last_poll_time = time.time()
    self.buffer_limit = 500
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
      if self.input_ports.keys() == []:
        self.task = [self.do_task(), self.master_port, [], 0]
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
    
    #Listen to output QUERY ports for async QUERY calls
    for p in self.output_ports:
      if p.port_type == Port.QUERY:
        socket = self.get_one(p.sockets)
        if socket == None:
          self.log(logging.DEBUG,
                   "has a port %s, url %d none" % (p.name, p.url))
          raise NameError
        self.log(logging.INFO, "Registering QUERY port: %s" % p.name)
        self.poller.register(socket, zmq.POLLIN)
    
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
      
      #TODO: hardcoded 2
      if time.time() - self.last_poll_time > 2:
        #update load values
        self.update_load()
        
      socks = dict(self.control_poller.poll(0))
      if socks != None and socks != {}:
        ports_with_data = [p for p in self.input_ports if p.socket in socks and socks[p.socket] == zmq.POLLIN]
              
        #process master instructions if any
        if socks.has_key(self.master_port.socket) and socks[self.master_port.socket] == zmq.POLLIN:
          message = json.loads(self.master_port.socket.recv())
          (control, data) = message
          self.log_recv(control, message, self.master_port)
          self.process_master(control, data)
      
      #no more pending tasks, now deal with data ports
      if self.task == None:
        socks = dict(self.poller.poll(500))
        if socks != None and socks != {}:
          input_ports_with_data = [p for p in self.input_ports if p.socket in socks and socks[p.socket] == zmq.POLLIN]
          output_ports_with_data = [p for p in self.output_ports if self.get_one(p.sockets) in socks and socks[self.get_one(p.sockets)] == zmq.POLLIN]
          ports_with_data = input_ports_with_data + output_ports_with_data
          # port_names = [p.name for p in ports_with_data]
          # if len(ports_with_data) > 0:
          #   self.log(logging.INFO, "Got data from ports: %r" % port_names)
          push_ports = [p for p in ports_with_data if p.port_type == Port.PUSH]
          query_input_ports = [p for p in ports_with_data if p.port_type == Port.QUERY and p in self.input_ports]
          query_output_ports = [p for p in ports_with_data if p.port_type == Port.QUERY and p in self.output_ports]
          
          while(self.task == None and push_ports != []):
            p = push_ports[0]
            push_ports = push_ports[1:]
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
          while(self.task == None and query_input_ports != []):
            p = query_input_ports[0]
            query_input_ports = query_input_ports[1:]
            message = p.socket.recv()
            (control, log) = json.loads(message)
            self.log_recv(control, message, p)
            if control == "END":
              self.process_stop(p, log)
            else:
              self.process_query(p, log)
          #there is no control message for QUERY results
          while(self.task == None and query_output_ports != []):
            p = query_output_ports[0]
            query_output_ports = query_output_ports[1:]
            log = self.get_one(p.sockets).recv()
            self.process_query_res(p, log)
  
  def get_load(self):
    requests_made = defaultdict(int)
    for p in self.output_ports:
      requests_made[p.name] = p.requests
    
    requests_served = defaultdict(int)
    for p in self.input_ports:
      requests_served[p.name] = p.requests

    return (requests_made, requests_served)
  
  def update_load(self):
    self.last_poll_time = time.time()  
    rm, rs = self.get_load()
    load = json.dumps(("ALIVE", rm, rs, self.total_processing_time, self.last_poll_time, os.getpid()))
    with open(self.poll_file_name, 'w') as f:
        f.write(load)
    
  def process_master(self, control, data):
    assert(False)
    self.log(logging.WARN, " Warning ** could not understand master")
  
  def process_push(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    requests = log.num_rows()
    start_time = time.time()
    res = self.recv_push(port.name, log)
    e = time.time()
    self.total_processing_time += (e - start_time)
    if res != None:
      self.task = [res, port, [], requests]
      return self.task
    else:
      port.requests += requests
  
  def process_buffered_push(self, port, logs):
    #print self.id + " got buffered push"
    for i,log in enumerate(logs):
      res = self.process_push(port, log)
      #task is not done yet, queue pending logs
      if res != None:
        self.task[2] = logs[i+1:]
        return
      #or this task is done but we still have pending logs and we haven't polled in a while
      #TODO: hardcoded 2
      elif (i < (len(logs) - 1) and time.time() - self.last_poll_time > 2):
        self.task = [None, port, logs[i+1:], 0]
        return
    
  def process_query(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    requests = log.num_rows()
    # print self.id + " got a query query for port " + port.name
    start_time = time.time()
    res = self.recv_query(port.name, log)
    e = time.time()
    self.total_processing_time += (e - start_time)
    if res != None:
      self.task = [res, port, [], requests]
      return self.task
    else:
      port.requests += requests
  
  def process_query_res(self, port, log_data):
    e = time.time()
    log = Log()
    log.set_log(log_data)
    #are we timing this?
    #port.start_time is set in query
    if self.pending_query_res_ports[port]:
      self.total_processing_time += (e - port.start_time)
    #remove this from pending ports
    del self.pending_query_res_ports[port]
    self.recv_query_result(port.name, log)
    
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
    task, port, logs, requests = self.task
    #we have pending buffered pushes
    if task == None:
      assert(logs != [])
      self.task = None
      self.process_buffered_push(port, logs)
    else:
      try:
        start_time = time.time()
        task.next()
        e = time.time()
        self.total_processing_time += (e - start_time)
      except StopIteration:
        if self.input_ports.keys() == []:
          self.shutdown()
        else:
          self.task = None
          port.requests += requests
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
    raise NotImplementedError("%s.do_task" % self.__class__.__name__)
  
  #TODO: doing this in a blocking fashion for now, can do it asynchronously
  def get_pending_query_results(self):
    if self.pending_query_res_ports.keys() != []:
      self.log(logging.INFO, "Waiting for pending query results to shutdown")
    else:
      self.log(logging.INFO, "No pending query results, ready to shutdown")

    #process_query_res might end up adding more pending queries, so deal with them all
    while(self.pending_query_res_ports.keys() != []):
      p = self.pending_query_res_ports.keys()[0]
      socket = self.get_one(p.sockets)
      log = json.loads(socket.recv())
      self.process_query_res(p, log)
    
  def on_shutdown(self):
    """Note that this is not called for crawler blocks.
    """
    pass

  def shutdown(self):
    self.flush_ports()
    self.get_pending_query_results()    
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
    self.last_poll_time = time.time()
    rm, rs = self.get_load()
    load = json.dumps(("SHUTDOWN", rm, rs, self.total_processing_time, self.last_poll_time, os.getpid()))
    with open(self.poll_file_name, 'w') as f:
        f.write(load)

  def on_load(self, config):
    raise NotImplementedError
  
  def add_port(self, port_name, port_type, keys_type, keys):
    port = Port(port_name, port_type, keys_type, keys)
    port.end_point = self
    self.ports.append(port)
    return port
  
  def recv_push(self, full_port_name, log):
    raise NotImplementedError("%s.recv_push" % self.__class__.__name__)
  
  def recv_query(self, port, log):
    raise NotImplementedError

  #This is overridden by individual blocks and by shard class
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
      raise Exception("could not find port with name: " + port_name)
    
    return port

  def push(self, port_name, log):
    assert self.current_buffer_size[port_name] == 0, \
      "Attempt to do an unbuffered push on port '%s' that has buffered data" % \
      port_name
    port = self.find_port(port_name)
    port.requests += log.num_rows()
    self.send("PUSH", log.log, port)
  
  def buffered_push(self, port_name, log):
    self.buffered_pushes[port_name].append(log.log)
    self.current_buffer_size[port_name] += log.num_rows()
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
  
  #query is blocking if async is set to False
  #add_time adds the time taken to run the query to this block's time
  #this is used mainly by the shards
  def query(self, port_name, log, async=False, add_time=False):
    port = self.find_port(port_name)
    if port.port_type != Port.QUERY:
      raise Exception("query did not get a QUERY port")
    port.requests += log.num_rows()
    self.send("QUERY", log.log, port)
    port.start_time = time.time()
    if not async:
      res = self.get_one(port.sockets).recv()
      e = time.time()
      if add_time:
        self.total_processing_time += (e - port.start_time)
      log_data = json.loads(res)
      log = Log()
      log.set_log(log_data)
      self.log_recv("QUERY response", res, port)
      return log
    else:
      if self.pending_query_res_ports.has_key(port):
        raise Exception("A query is already pending on this port")
      self.pending_query_res_ports[port] = add_time
      return None
  
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
