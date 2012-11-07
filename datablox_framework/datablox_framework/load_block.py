import json
import sys
import os
import logging

import naming
from block import *
from shard import *


try:
  import datablox_engage_adapter.file_locator
  using_engage = True
  print "[load_block] Found datablox_engage_adapter, using Engage"
  sys.stdout.flush()
except ImportError:
  using_engage = False
  print "[load_block] Did not find datablox_engage_adapter, not using Engage"
  sys.stdout.flush()
  

def read_configuration(configuration_file_name):
  with open(configuration_file_name) as f:
    return json.load(f)

def get_one(_list):
  assert(len(_list) == 1)
  return _list[0]
  
def is_shard(block_name):
  return naming.block_class_name(block_name).endswith('_shard')


def setup_policy(inst, policy):
  for (k, v) in policy.items():
    if k == "queue_size":
      inst.set_queue_size(v)
    else:
      print "Unknown policy type " + k
      raise NameError


def start(blox_dir, configuration_file_name, poll_file_name, log_dir):
  global using_engage
  try:
    sys.path.index(blox_dir)
  except ValueError:
    sys.path.append(blox_dir)

  config = read_configuration(configuration_file_name)

  block_version = config["version"] if config.has_key("version") \
                                    else naming.DEFAULT_VERSION

  block_class = \
    naming.get_block_class(config["name"], block_version)
  print "[load_block] Preparing to start block %s" % config["name"]
  sys.stdout.flush()
  
  inst = block_class(config["master_port"])
  inst.id = config["id"]
  inst.block_name = config["name"]
  # We set the thread's name to be the block id
  inst.name = config["id"]
  inst.ip_address = config["ip_address"]
  inst.poll_file_name = poll_file_name
  inst.log_level = config["log_level"]
  # initialize logging
  inst.initialize_logging(log_directory=log_dir)
  inst.log(logging.DEBUG, config)
  try:
    #setup policies
    if config.has_key("policy"):
      setup_policy(inst, config["policy"])

    inst.on_load(config["args"])

    if is_shard(config["name"]):
      num_blocks = config["num_blocks"]
      inst.num_nodes = num_blocks
      port_type = Port.PUSH if config["port_type"] == "PUSH" else Port.QUERY
      for i in range(num_blocks):
        output_port = "output"+str(i)
        inst.add_port(output_port, port_type, Port.UNNAMED, [])

    for (port_name, port_config) in config["ports"].items():
      port_outlet, port_nums = port_config[0], port_config[1:]
      #TODO: loop does extra work, rewrite this
      for port_num in port_nums:
        if port_outlet == "output":
          inst.add_output_connection(port_name, port_num)
        elif port_outlet == "input":
          inst.add_input_connection(port_name, port_num)
        else:
          print "Unknown port outlet " + port_outlet
          raise NameError

    #for dynamic join
    if config.has_key("subscribers"):
      inst.set_subscribers(config["subscribers"])

    inst.start()
  except Exception, e:
    inst.logger.exception("Uncaught exception in block %s: %s" %
                          (config["name"], e))
    raise

  
if __name__ == "__main__":
  blox_dir = sys.argv[1]
  configuration_file_name = sys.argv[2]
  poll_file_name = sys.argv[3]
  if len(sys.argv)>4:
    log_dir=sys.argv[4]
  else:
    log_dir=None
  
  start(blox_dir, configuration_file_name, poll_file_name, log_dir=log_dir)
