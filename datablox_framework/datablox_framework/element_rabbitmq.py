import pika
from pika.adapters import SelectConnection
import threading
import json

class Log(object):
  def __init__(self):
    self.log = {}
  
  def set_log(self, log):
    self.log = log
  
  
class Port(object):
      PULL = 0
      PUSH = 1
      AGNOSTIC = 2
      
      NAMED = 0
      UNNAMED = 1
      
      def __init__(self, port_name, port_type, keys_type, keys):
        self.name = port_name
        self.port_type = port_type
        self.keys_type = keys_type
        #do some additional checks here - length(key) = 1 if port_type = named
        self.keys = keys
        self.end_point = None
      
      def connect_to(self, element):
        self.end_point = element

class InvalidConnection(Exception):
  def __init__(self, fromc, toc):
    self.fromc = fromc
    self.toc = toc
    
  def __str__(self):
    return "Invalid connection from " + fromc.name + " to " + toc.name
    
  
class Element(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
    self.name = "__no_name__"
    self.connection_type = Port.AGNOSTIC
    self.ports = []
    self.connections = []
    self.input_connections = []
    #pika initialization
    self.channel = None
    self.queues_declared = 0

  def run(self):
    parameters = pika.ConnectionParameters()
    self.connection = SelectConnection(parameters, self.pika_on_connected)
    #self.connection.set_backpressure_multiplier(value=1)
    try:
      # Loop so we can communicate with RabbitMQ
      self.connection.ioloop.start()
      print "WARNING: %s ioloop ended" % self.name
    except KeyboardInterrupt:
      # Gracefully close the connection
      self.connection.close()
      # Loop until we're fully closed, will stop on its own
      self.connection.ioloop.start()

  def pika_on_connected(self, connection):
    """Called when we are fully connected to RabbitMQ"""
    connection.channel(self.pika_on_channel_open)
  
  def pika_on_channel_open(self, new_channel):
    # print dir(new_channel.basic_qos)
    """Called when our channel has opened"""
    self.channel = new_channel
    self.channel.basic_qos(callback=None, prefetch_size=0, prefetch_count=50, global_=False)
    # declare queues we might send data to and receive data from
    for c in self.connections:
      self.queues_declared = self.queues_declared + 1
      self.channel.queue_declare(queue=c[1].name, callback=self.pika_on_queue_declared)
      if c[0].port_type == Port.PULL:
        self.queues_declared = self.queues_declared + 1
        self.channel.queue_declare(queue=c[0].name, callback=self.pika_on_queue_declared)
    for c in self.input_connections:
      self.queues_declared = self.queues_declared + 1
      self.channel.queue_declare(queue=c[0].name, callback=self.pika_on_queue_declared)
  
  def our_port(self, port_name):
    for p in self.ports:
      if p.name == port_name:
        return True
    return False
    
  def pika_on_queue_declared(self, frame):
    self.queues_declared = self.queues_declared - 1
    if self.our_port(frame.method.queue):
      # print self.name + " listening to " + frame.method.queue
      self.channel.basic_consume(self.pika_callback, queue=frame.method.queue, no_ack=True)
    if self.queues_declared == 0:
      self.src_start()
    
  def pika_callback(self, ch, method, properties, body):
    control, log_data = json.loads(body)
    #ch.basic_ack(delivery_tag = method.delivery_tag)
    if control == "push":
      log = Log()
      log.set_log(log_data)
      # print self.name + " got a push for port " + method.routing_key
      self.recv_push(method.routing_key, log)
    elif control == "pull_query":
      log = Log()
      log.set_log(log_data)
      port_name = method.routing_key
      print self.name + " got a pull query for port " + port_name
      self.recv_pull_query(port_name, log)
    elif control == "pull_result":
      log = Log()
      log.set_log(log_data)
      print self.name + " got a pull result for port " + method.routing_key
      self.recv_pull_result(method.routing_key, log)
    # remove this port from the incoming connections as it stopped
    elif control == "stop":
      print self.name + " got stop message from " + log_data + " for port " + method.routing_key 
      new_input_connections = []
      for c in self.input_connections:
        if c[0].name == method.routing_key and c[1].name == log_data:
          print "removing connection %s to %s" % (c[0].name, c[1].name)
          continue
        new_input_connections.append(c)
      if new_input_connections == self.input_connections:
        print "(warning)" + self.name + " got stop from unknown port, known ports: " + str(self.input_connections)
      self.input_connections = new_input_connections
      if new_input_connections == []:
        self.shutdown()
    else:
      assert(False)

  def send(self, message, connections):
    json_log = json.dumps(message)
    for c in connections:
      # print self.name + " publishing to " + c[1].name
      self.channel.basic_publish(exchange='',
                            routing_key=c[1].name,
                            body=json_log)
  
  def on_shutdown(self):
    pass

  def shutdown(self):
    print "Got a stop request, trying to stop thread " + self.name
    for c in self.connections:
      message = ("stop", c[0].name)
      json_log = json.dumps(message)
      # print self.name + " asking to stop connection to " + c[1].name
      self.channel.basic_publish(exchange='',
                            routing_key=c[1].name,
                            body=json_log)
    # self.send(message, self.connections)
    # raising KeyboardInterrupt this right now as run() catches it and does the right thing
    # TODO: define a special interrupt class for this
    # self.on_shutdown()
    # Gracefully close the connection
    print self.name + " closing connection"
    self.connection.close()
    # Loop until we're fully closed, will stop on its own
    print self.name + " doing a last IO loop"
    self.connection.ioloop.start()
    print self.name + " done IO loop"
    
  def src_start(self):
    """Sources' can start sending data"""
    pass
    
  def teardown(self):
    """Called before unloading the element"""
    raise NotImplementedError
    
  def on_load(self, config):
    raise NotImplementedError
  
  def full_port_name(self, port_name):
    return self.name + port_name
    
  def add_port(self, port_name, port_type, keys_type, keys):
    full_port_name = self.full_port_name(port_name)
    port = Port(full_port_name, port_type, keys_type, keys)
    port.end_point = self
    self.ports.append(port)
  
  def recv_push(self, full_port_name, log):
    raise NotImplementedError
  
  def recv_pull_query(self, port, log):
    raise NotImplementedError

  def recv_pull_result(self, port, log):
    raise NotImplementedError
  
  def find_port(self, full_port_name):
    port = None
    for p in self.ports:
      if p.name == full_port_name:
        port = p
    
    if port == None:
      print self.name + " could not find port with name: " + full_port_name
      raise NameError
    
    return port

  def find_connections(self, full_output_port_name):
    connections = []
    for c in self.connections:
      if c[0].name == full_output_port_name:
        connections.append(c)
    
    if connections == []:
      print self.name + " could not find connection with port " + full_output_port_name
      raise NameError
    
    return connections

  def find_input_connections(self, full_input_port_name):
    connections = []
    for c in self.input_connections:
      if c[0].name == full_input_port_name:
        connections.append(c)

    if connections == []:
      print self.name + " could not find input connection with port " + full_input_port_name
      raise NameError

    return connections
    
  def push(self, port_name, log):
    full_port_name = self.full_port_name(port_name)
    connections = self.find_connections(full_port_name)
    message = ("push", log.log)
    self.send(message, connections)
  
  # cannot broadcast pulls
  def pull(self, port_name, log):
    full_port_name = self.full_port_name(port_name)
    connections = self.find_connections(full_port_name)
    assert(len(connections) == 1)
    message = ("pull_query", log.log)
    self.send(message, connections)
  
  def return_pull(self, full_port_name, log):
    print self.name + " returning pull result on " + full_port_name
    connections = self.find_input_connections(full_port_name)
    assert(len(connections) == 1)
    message = ("pull_result", log.log)
    self.send(message, connections)

  def connect(self, output_port_name, element, input_port_name):
    output_port = self.find_port(self.full_port_name(output_port_name))
    input_port = element.find_port(element.full_port_name(input_port_name))
    self.connections.append((output_port, input_port))
    element.input_connections.append((input_port, output_port))