import json
import sys
import os

import naming
from element import *
from shard import *

try:
  import datablox_engage_adapter.install
  using_engage = True
except ImportError:
  using_engage = False


def read_configuration(configuration_file_name):
  with open(configuration_file_name) as f:
    return json.load(f)

def get_one(_list):
  assert(len(_list) == 1)
  return _list[0]
  
def is_shard(element_name):
  return naming.element_class_name(element_name).endswith('_shard')


def start(blox_dir, configuration_file_name):
  try:
    sys.path.index(blox_dir)
  except ValueError:
    sys.path.append(blox_dir)

  config = read_configuration(configuration_file_name)
  print config

  block_version = config["version"] if config.has_key("version") \
                                    else naming.DEFAULT_VERSION
  if using_engage:
    resource_key = naming.get_block_resource_key(config["name"],
                                                 block_version)
    datablox_engage_adapter.install.install_block(resource_key)
  element_class = \
    naming.get_block_class(config["name"], block_version)
  inst = element_class(config["master_port"])
  inst.on_load(config["args"])

  if is_shard(config["name"]):
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
