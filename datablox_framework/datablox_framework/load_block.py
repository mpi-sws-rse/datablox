from element import *
from shard import *
import json
import sys
import os

def read_configuration(configuration_file_name):
  with open(configuration_file_name) as f:
    return json.load(f)

def get_one(_list):
  assert(len(_list) == 1)
  return _list[0]
  
def is_shard(element_name):
  return element_name.endswith('_shard')

def element_module(element_name):
  return 'e_' + element_name.lower().replace('-', '_')

def element_class_name(element_name):
  return element_name.lower().replace('-', '_')

def start(blox_dir, configuration_file_name):
  try:
    sys.path.index(blox_dir)
  except ValueError:
    sys.path.append(blox_dir)

  config = read_configuration(configuration_file_name)
  print config
  
  module_name = element_module(config["name"])
  element_name = element_class_name(config["name"])
  module = __import__(module_name)
  element_class = getattr(module, element_name)
  inst = element_class(config["master_port"])
  inst.on_load(config["args"])

  if is_shard(element_name):
    num_elements = config["num_elements"]
    inst.num_nodes = num_elements
    for i in range(num_elements):
      output_port = "output"+str(i)
      inst.add_port(output_port, Port.PUSH, Port.UNNAMED, [])

  for (port_name, port_config) in config["ports"].items():
    port_type, port_nums = port_config[0], port_config[1:]
    #TODO: loop does extra work, rewrite this
    for port_num in port_nums:
      if port_type == "output":
        inst.add_output_connection(port_name, port_num)
      elif port_type == "input":
        inst.add_input_connection(port_name, port_num)
      else:
        print "Unknown port type " + port_type
        raise NameError

  #for dynamic join
  if config.has_key("subscribers"):
    inst.set_subscribers(config["subscribers"])
  
  inst.start()

if __name__ == "__main__":
  blox_dir = sys.argv[1]
  configuration_file_name = sys.argv[2]
  
  start(blox_dir, configuration_file_name)