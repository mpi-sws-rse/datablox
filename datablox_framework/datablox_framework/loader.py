"""This is the command line interface to the master.
"""
import os
import os.path
import sys
from optparse import OptionParser
import logging
from logging.handlers import RotatingFileHandler

from master import *

try:
  import datablox_engage_adapter.file_locator
  using_engage = True
except ImportError:
  using_engage = False

if using_engage:
  engage_file_locator = datablox_engage_adapter.file_locator.FileLocator()
  print "Running with Engage deployment home at %s" % \
    engage_file_locator.get_dh()
  import datablox_engage_adapter.install
else:
  engage_file_locator = None


def build_args(flat_args):
  """Given flattened arguments provided by the user
  """
  args = {} # map where key is block id, value is a map from arg name to arg value
  for k in flat_args.keys():
    k_comps = k.split(".")
    if (len(k_comps) < 2) or (len(k_comps) > 3):
      raise Exception("invalid key %s: should have form [group_id].block_id.arg_name" % k)
    if len(k_comps)==2:
      gid = "main"
      bid = k_comps[0]
      an = k_comps[1]
    else:
      gid = k_comps[0]
      bid = k_comps[1]
      an = k_comps[2]
    if not args.has_key(gid):
      args[gid] = {}
    group = args[gid]
    if not group.has_key(bid):
      group[bid] = {}
    block = group[bid]
    block[an] = flat_args[k]
  return args
  
log_levels = {
  "ERROR": logging.ERROR,
  "WARN": logging.WARN,
  "INFO": logging.INFO,
  "DEBUG": logging.DEBUG,
  "ALL": 1
}

def main(argv):
  if using_engage:
    usage = "%prog [options] config_file node_name_1 node_name_2 ..."
  else:
    usage = "%prog [options] config_file ip_address1 ip_address2 ..."
  parser = OptionParser(usage=usage)
  parser.add_option("-a", "--args", dest="args", default=None,
                    help="JSON map containing overrides of block arguments in the topology. The keys in the map have the form [group_id].block_id.arg_name. Example: --args '{\"source.sleep\":5, \"sink.sleep\":10}' Note that --args is mutually exclusive with --args-file.")
  parser.add_option("--args-file", dest="args_file", default=None,
                    help="File containing JSON map containing overrides of block arguments in the topology. The keys in the map have the form [group_id].block_id.arg_name. Mutually exclusive with --args.")
  parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                    help="use this path instead of the environment variable BLOXPATH")
  parser.add_option("-p", "--poll-interval", dest="poll_interval", type="int",
                    default=DEFAULT_POLL_INTERVAL,
                    help="Time in seconds between polls of the caretakers, defaults to %d" %
                    DEFAULT_POLL_INTERVAL)
  parser.add_option('--stats-multiple', dest='stats_multiple', type="int",
                    default=DEFAULT_STATS_MULTIPLE,
                    help="Number of polls between stats gathering, defaults to %d" %
                    DEFAULT_STATS_MULTIPLE)
  parser.add_option("-l", "--log-level", dest="log_level", default="INFO",
                    help="Log level: ERROR|WARN|INFO|DEBUG|ALL")
  parser.add_option("-d", "--debug-blocks", dest="debug_blocks", default=None,
                    help="Comma-separated list of blocks for which to enable debug-level logging")
  parser.add_option("--loads-file", dest="loads_file", default=None,
                    help="If specified, write block load history to this file in csv format at end of run")
  parser.add_option('--log-stats-hist', dest='log_stats_hist', default=False,
                    action="store_true",
                    help="If specified, save gathered performance statistics to the log file")
  if using_engage:
    parser.add_option("--reuse-existing-installs", default=None,
                      action="store_true",
                      help="Reuse existing Datablox installs on worker nodes, if present (This is the default behavior).")
    parser.add_option("--always-reinstall-workers", default=False,
                      action="store_true",
                      help="Always reinstall Datablox on worker nodes")
  (options, args) = parser.parse_args(argv)
  if using_engage and options.reuse_existing_installs and \
         options.always_reinstall_workers:
    parser.error("--reuse-existing-installs and --always-reinstall-workers are mutually exclusive")
  elif using_engage and options.always_reinstall_workers==False:
    reuse_existing_installs = True
  else:
    reuse_existing_installs = False

  if len(args)<1:
    parser.error("Need to specify config file and nodes.")
  elif len(args)<2: #and options.pool==None:
    parser.error("Need to specify list of nodes/ip_addreses or a DJM pool name")

  bloxpath = options.bloxpath

  if options.args and options.args_file:
    parser.error("Cannot specify both --args and --args-file")
  if options.args:
    try:
      block_args_flat = json.loads(options.args)
    except Exception, e:
      parser.error("--args option value %s is not valid JSON: %s" % (options.args, e))
    try:
      block_args = build_args(block_args_flat)
    except Exception, e:
      parser.error("--args option: %s" % e)
  elif options.args_file:
    args_filepath = os.path.abspath(os.path.expanduser(options.args_file))
    if not os.path.exists(args_filepath):
      parser.error("--args-file: file %s does not exist" % args_filepath)
    try:
      with open(args_filepath, "rb") as f:
        block_args_flat = json.load(f)
    except Exception, e:
      parser.error("--args-file %s is not valid JSON: %s" % (args_filepath, e))
    try:
      block_args = build_args(block_args_flat)
    except Exception, e:
      parser.error("--args-file option: %s" % e)
  else:
    block_args = None
  if options.debug_blocks:
    debug_block_list = options.debug_blocks.split(',')
  else:
    debug_block_list = []
      
    
  # The priorities for obtaining bloxpath are:
  # 1. Command line option
  # 2. If engage is installed, use file locator to get bloxpath
  # 3. Otherwise, lookfor BLOXPATH environment variable
  if bloxpath == None:
    if using_engage:
      bloxpath = engage_file_locator.get_blox_dir()
    elif not os.environ.has_key("BLOXPATH"):
      parser.error("Need to set BLOXPATH environment variable or pass it as an argument")
    else:
      bloxpath = os.environ["BLOXPATH"]

  if not os.path.isdir(bloxpath):
    parser.error("BLOXPATH %s does not exist or is not a directory" % bloxpath)

  if options.log_level not in log_levels.keys():
    parser.error("--log-level must be one of %s" % log_levels.keys())

  # setup logging
  root_logger = logging.getLogger()
  if len(root_logger.handlers)==0:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_levels[options.log_level])
    root_logger.addHandler(console_handler)
  if using_engage:
    root_logger.setLevel(min(root_logger.level, logging.DEBUG))
    log_dir = engage_file_locator.get_log_directory()
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "master.log")
    do_log_rollover = os.path.exists(log_file)
    handler = RotatingFileHandler(log_file, backupCount=5)
    if do_log_rollover: # we do a rollover each time the master is run
      handler.doRollover()
    handler.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
  else:
    root_logger.setLevel(min(root_logger.level, log_levels[options.log_level]))
    
  Master(bloxpath, args[0], args[1:], using_engage,
         _log_level=log_levels[options.log_level],
         _debug_block_list=debug_block_list,
         reuse_existing_installs=reuse_existing_installs,
         poll_interval=options.poll_interval,
         stats_multiple=options.stats_multiple,
         block_args=block_args,
         loads_file=options.loads_file,
         log_stats_hist=options.log_stats_hist)

def call_from_console_script():
    sys.exit(main(sys.argv[1:]))

if __name__ == "__main__":
  main(sys.argv[1:])
