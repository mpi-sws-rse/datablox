import os
import os.path
import sys
from optparse import OptionParser
import logging

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
  parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                    help="use this path instead of the environment variable BLOXPATH")
  parser.add_option("-l", "--log-level", dest="log_level", default="INFO",
                    help="Log level: ERROR|WARN|INFO|DEBUG|ALL")
                    
  (options, args) = parser.parse_args(argv)

  if len(args)<1:
    parser.error("Need to specify config file and nodes.")
  elif len(args)<2 and options.pool==None:
    parser.error("Need to specify list of nodes/ip_addreses or a DJM pool name")

  bloxpath = options.bloxpath

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
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
  else:
    root_logger.setLevel(min(root_logger.level, log_levels[options.log_level]))
    
  Master(bloxpath, args[0], args[1:], using_engage,
         _log_level=log_levels[options.log_level])

def call_from_console_script():
    sys.exit(main(sys.argv[1:]))

if __name__ == "__main__":
  main(sys.argv[1:])
