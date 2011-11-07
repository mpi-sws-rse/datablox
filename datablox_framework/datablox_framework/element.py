import zmq
import time
import json
import threading
import collections

class Log(object):
  def __init__(self):
    self.log = {}
  
  def set_log(self, log):
    self.log = log
  
  
class Port(object):
      PULL = 0
      PUSH = 1
      AGNOSTIC = 2
      MASTER = 3
      
      NAMED = 0
      UNNAMED = 1
      
      def __init__(self, port_name, port_type, keys_type, keys, port_number):
        self.name = port_name
        self.port_type = port_type
        self.keys_type = keys_type
        #do some additional checks here - length(key) = 1 if port_type = named
        self.keys = keys
        self.end_point = None
        self.port_number = port_number
        self.socket = None
      
      def connect_to(self, element):
        self.end_point = element

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
  
  #leave one port number for control
  def new_port(self):
    self.port_num += 2
    return self.port_num
    
class Element(threading.Thread):
  def __init__(self, master_port_num, port_num_gen):
    threading.Thread.__init__(self)
    self.name = "__no_name__"
    self.connection_type = Port.AGNOSTIC
    master_port = Port("master", Port.MASTER, Port.UNNAMED, [], master_port_num)
    self.master_port = master_port
    self.ports = [master_port]
    self.connections = []
    self.input_connections = []
    self.port_num_generator = port_num_gen
    self.context = None
    self.poller = None
    self.alive = True

  def run(self):
    try:
      self.context = zmq.Context()
      self.ready_ports()
      print self.name + " ports are ready"
      self.src_start()
      if self.input_connections != []:
        self.start_listening()
    except KeyboardInterrupt:
      print "Stopping thread"
  
  def ready_ports(self):
    self.listen_to_master()
    
    #we can have multiple connections from the same output port
    #get the number of subscribers for each connection
    output_port_hash = collections.defaultdict(int)
    for c in self.connections:
      if c[0].port_type != Port.PULL:
        output_port_hash[c[0]] += 1
      
    #this won't be a problem for input pull connections
    #as they are assumed to be 1-1
    input_bind_ports = [c[0] for c in self.input_connections if c[0].port_type == Port.PULL]
    
    #we serve all output ports
    for p in output_port_hash.keys():
      self.bind_pub_port(p)
      
    #we also serve input pull ports
    for p in input_bind_ports:
      self.bind_server_port(p)
      
    #wait till we have elements listening to us
    for p, subscribers in output_port_hash.items():
      self.wait_for_subscribers(p, subscribers)

    #Tell all ports we expect data from that we are ready
    #PULL ports are not included in this because they work on REP-REQ protocol 
    #   which waits for both parties to be ready
    input_listen_ports = [c[0] for c in self.input_connections if c[0].port_type != Port.PULL]
    for p in input_listen_ports:
      self.subscribe_to_server(p)
 
    self.poller = zmq.Poller()
    #Listen to Master
    self.poller.register(self.master_port.socket, zmq.POLLIN)
    
    #Listen to all inputs
    for p in [c[0] for c in self.input_connections]:
      self.poller.register(p.socket, zmq.POLLIN)
    
    #Connect to all pull-output ports
    for c in self.connections:
      port = c[0]
      if port.port_type == Port.PULL:
        port.socket = self.context.socket(zmq.REQ)
        port.socket.connect(self.listen_url(port.port_number))
    
  
  def listen_to_master(self):
    self.bind_server_port(self.master_port)
    print self.name + " waiting for master to respond"
    #wait for master to synchronize
    self.master_port.socket.recv()
    print self.name + " master's online, starting other ports"
    self.master_port.socket.send('')
    
  def bind_url(self, port_number):
    return "tcp://*:" + str(port_number)
  
  def listen_url(self, port_number):
    return "tcp://localhost:" + str(port_number)

  def bind_pub_port(self, port):
    bind_url = self.bind_url(port.port_number)
    port.socket = self.context.socket(zmq.PUB)
    port.socket.bind(bind_url)
  
  def bind_server_port(self, port):
    bind_url = self.bind_url(port.port_number)
    port.socket = self.context.socket(zmq.REP)
    port.socket.bind(bind_url)

  def subscribe_to_server(self, port):
    listen_url = self.listen_url(port.port_number)
    port.socket = self.context.socket(zmq.SUB)
    port.socket.connect(listen_url)
    port.socket.setsockopt(zmq.SUBSCRIBE, "")

    #control ports are one more than data ports
    control_url = self.listen_url(port.port_number+1)
    syncclient = self.context.socket(zmq.REQ)
    syncclient.connect(control_url)
    syncclient.send('')
    # wait for synchronization reply
    syncclient.recv()
  
  def wait_for_subscribers(self, port, expected_subscribers):
    #control ports are one more than data ports
    control_url = self.bind_url(port.port_number+1)
    # Socket to receive signals
    syncservice = self.context.socket(zmq.REP)
    syncservice.bind(control_url)

    # Get synchronization from subscribers
    subscribers = 0
    while subscribers < expected_subscribers:
        # wait for synchronization request
        msg = syncservice.recv()
        # send synchronization reply
        syncservice.send('')
        subscribers += 1
        print self.name + " +1 subscriber"
      
  def start_listening(self):
    while self.alive:
      socks = dict(self.poller.poll())
      ports_with_data = [p for p in self.ports if p.socket in socks and socks[p.socket] == zmq.POLLIN]
      push_ports = [p for p in ports_with_data if p.port_type == Port.PUSH]
      pull_ports = [p for p in ports_with_data if p.port_type == Port.PULL]

      #process master instructions if any
      if socks.has_key(self.master_port.socket) and socks[self.master_port.socket] == zmq.POLLIN:
        message = json.loads(self.master_port.socket.recv())
        self.process_master(message)
        
      for p in push_ports:
        message = p.socket.recv()
        (control, log) = json.loads(message)
        if control == "END":
          self.process_stop(p, log)
        else:
          self.process_push(p, log)
      for p in pull_ports:
        message = p.socket.recv()
        (control, log) = json.loads(message)
        if control == "END":
          self.process_stop(p, log)
        else:
          self.process_pull_query(p, log)    
  
  def process_master(self, control_data):
    if control_data == "POLL":
      load = json.dumps(1000)
      self.master_port.socket.send(load)
    else:
      print self.name + " Warning ** could not understand master"
    
  def process_push(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    self.recv_push(port.name, log)
  
  def process_pull_query(self, port, log_data):
    log = Log()
    log.set_log(log_data)
    print self.name + " got a pull query for port " + port.name
    self.recv_pull_query(port.name, log)
  
  def process_stop(self, port, stop_msg):
    (element_name, recv_port_name) = stop_msg
    print "(%s, %s) stopped on (%s, %s)" % (element_name, recv_port_name, self.name, port.name)
    #we could have multiple (element, port) input pairs, remove only one
    new_connections = []
    removed = False
    for c in self.input_connections:
      if (not removed) and (c[0].name, c[1].name, c[1].end_point.name) == (port.name, recv_port_name, element_name):
        removed = True
      else:
        new_connections.append(c)
    
    if new_connections == self.input_connections:
      print "WARNING: %s received a stop from a stopped/unknown element %s" % (self.name, element_name)
    else:
      self.input_connections = new_connections
    
    if self.input_connections == []:
      self.on_shutdown()
      self.shutdown()
    
  def send(self, control, message, port):
    message = (control, message)
    json_log = json.dumps(message)
    port.socket.send(str(json_log))
  
  def on_shutdown(self):
    pass

  def shutdown(self):
    outputs = {}
    for c in self.connections:
      outputs[c[0]] = True
    
    for p in outputs.keys():
      self.send("END", (self.name, p.name), p)
    self.alive = False

  def src_start(self):
    """Sources can start sending data"""
    pass
    
  def on_load(self, config):
    raise NotImplementedError
  
  def add_port(self, port_name, port_type, keys_type, keys):
    port_number = self.port_num_generator.new_port()
    port = Port(port_name, port_type, keys_type, keys, port_number)
    port.end_point = self
    self.ports.append(port)
  
  def recv_push(self, full_port_name, log):
    raise NotImplementedError
  
  def recv_pull_query(self, port, log):
    raise NotImplementedError

  def recv_pull_result(self, port, log):
    raise NotImplementedError
  
  def find_port(self, port_name):
    port = None
    for p in self.ports:
      if p.name == port_name:
        port = p
    
    if port == None:
      print self.name + " could not find port with name: " + port_name
      raise NameError
    
    return port

  def push(self, port_name, log):
    port = self.find_port(port_name)
    self.send("PUSH", log.log, port)
  
  #pull is blocking for now
  def pull(self, port_name, log):
    port = self.find_port(port_name)
    self.send("PULL", log.log, port)
    res = port.socket.recv()
    log_data = json.loads(res)
    log = Log()
    log.set_log(log_data)
    print self.name + " got a pull result for port " + port_name
    return log
  
  def return_pull(self, port_name, log):
    port = self.find_port(port_name)
    log_data = json.dumps(log.log)
    port.socket.send(log_data)
    
  def already_connected(self, input_port_name):
    for c in self.input_connections:
      if c[0].name == input_port_name:
        return True
    return False
    
  def connect(self, output_port_name, element, input_port_name):
    if element.already_connected(input_port_name):
      print "Connecting to an already connected input port"
      raise NameError
    output_port = self.find_port(output_port_name)
    input_port = element.find_port(input_port_name)
    #they will now have the same port number
    input_port.port_number = output_port.port_number
    self.connections.append((output_port, input_port))
    element.input_connections.append((input_port, output_port))