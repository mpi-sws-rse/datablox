import sys
import os
import zmq
import xmlrpclib
import json
from SimpleXMLRPCServer import SimpleXMLRPCServer

server = SimpleXMLRPCServer(("localhost", 8000))
print "Listening on port 8000..."

with open(sys.argv[1], 'r') as f:
  connections = json.load(f)
  print connections

port_sockets = {}
context = zmq.Context()
quit = False

for port_name, rest in connections.items():
  assert(rest[0] == "output")
  connection_url = rest[1]
  port_sockets[port_name] = context.socket(zmq.REQ)
  port_sockets[port_name].connect(connection_url)
  
def process_port(port_name, args):
  socket = port_sockets[port_name]
  args = json.loads(args)
  print "Got request at port: %s, with args %r" % (port_name, args)
  control = "QUERY"
  message = (control, args)
  json_log = json.dumps(message)
  socket.send(json_log)
  res = socket.recv()
  return res

def shutdown():
  global quit
  control = "END"
  for p, socket in port_sockets.items():
    print "Ending ", p
    args = ("RPC", p)
    message = (control, args)
    json_log = json.dumps(message)
    socket.send(json_log)
  quit = True
  return True
  
for port_name in connections.keys():
  #see http://mail.python.org/pipermail/tutor/2005-November/043360.html
  server.register_function(lambda args,_p=port_name: process_port(_p, args), port_name)

server.register_function(shutdown, 'shutdown')

while not quit:
  server.handle_request()

sys.exit(0)