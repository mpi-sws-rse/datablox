import zmq
import json
import os
import os.path
import sys
import subprocess
import signal
from optparse import OptionParser
from fileserver import file_server_keypath
import string
import logging

logger = logging.getLogger(__name__)

import naming

try:
  import datablox_engage_adapter.file_locator
  using_engage = True
except ImportError:
  using_engage = False

if using_engage:
  engage_file_locator = datablox_engage_adapter.file_locator.FileLocator()
  import datablox_engage_adapter.install
else:
  engage_file_locator = None

class CareTaker(object):
  def __init__(self, argv):
    self.processes = {}
    self.fileserver_process = None
    self.socket = None
    self.bloxpath = None
    self.config_dir = None
    self.log_dir = None
    self.file_num = 0
    self.setup(argv)

  def stop_all(self):
    logger.info("[caretaker] stopping all blocks")
    for p in self.processes.keys():
      p.terminate()
    logger.info("[caretaker] done")

  def shutdown(self):
    self.stop_all()
    if self.fileserver_process:
      self.fileserver_process.terminate()
    self.socket.close()
    sys.exit(0)
  
  def sigterm_handler(self, signum, frame):
    logger.info("[caretaker] got SIGTERM")
    self.shutdown()

  def start_fileserver(self):
    #with open(file_server_keypath, 'w') as f:
    #  f.write(gen_random(8))

    fileserver_script = os.path.join(os.path.dirname(__file__),
                                     "fileserver.py")
    command = [sys.executable, fileserver_script]
    self.fileserver_process = subprocess.Popen(command)
  
  def start_block(self, data):
    try:
      if using_engage:
        block_name = data["name"]
        block_version = data["version"] if data.has_key("version") \
                        else naming.DEFAULT_VERSION
        resource_key = naming.get_block_resource_key(block_name,
                                                     block_version)
        logger.info("Using engage to install resource %s" % resource_key)
        datablox_engage_adapter.install.install_block(resource_key)
        logger.info("Install of %s and its dependencies successful" % \
                    resource_key)
      config_name = os.path.join(self.config_dir,
                                 data["name"] + str(self.file_num) + ".json")
      poll_file_name = os.path.join(self.config_dir,
                                 data["name"] + str(self.file_num) + "_poll.json")
      self.file_num += 1
      with open(config_name, 'w') as config_file:
        json.dump(data, config_file)
      load_block_script = os.path.join(os.path.dirname(__file__),
                                       "load_block.py")
      command = [sys.executable, load_block_script, self.bloxpath, config_name, poll_file_name]
      if self.log_dir:
        command.append(self.log_dir)
      logger.debug("Running command %s" % command)
      p = subprocess.Popen(command)
      self.processes[p] = [data["id"], poll_file_name]
    except Exception, e:
      logger.exception("Got exception %s when processing ADD BLOCK message" % e)
      return False
    return True

  def collect_poll_data(self):
    loads = {}
    for block_id, block_file in self.processes.values():
      try:
        block_load = None
        with open(block_file, 'r') as f:
          s = f.read()
          block_load = json.loads(s)
        loads[block_id] = block_load
      #TODO: try to re-read the file as the block could have been writing to it at this time
      except Exception, e:
        print e
        continue
    return loads
    
  def setup(self, argv):
    # setup logging
    root_logger = logging.getLogger()
    if len(root_logger.handlers)==0:
      console_handler = logging.StreamHandler(sys.stdout)
      if using_engage:
        log_level = logging.DEBUG # stdout is going to a file anyway
      else:
        log_level = logging.INFO
      console_handler.setLevel(log_level)
      root_logger.addHandler(console_handler)
      root_logger.setLevel(log_level)

    usage = "%prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                      help="use this path instead of the environment variable BLOXPATH")
    parser.add_option("--config-dir", dest="config_dir", default=".",
                      help="directory to use for storing configuration files for the individual blocks")
    parser.add_option("--log-dir", dest="log_dir", default=None,
                       help="Directory to use for log files, if not specified just use the console")

    (options, args) = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, self.sigterm_handler)
    self.bloxpath = options.bloxpath
  
    if self.bloxpath == None: 
      if not os.environ.has_key("BLOXPATH"):
        parser.error("Need to set BLOXPATH environment variable or pass it as an argument")
      else:
        self.bloxpath = os.environ["BLOXPATH"]

    if not os.path.isdir(self.bloxpath):
      parser.error("BLOXPATH %s does not exist or is not a directory" % self.bloxpath)

    self.config_dir = os.path.abspath(os.path.expanduser(options.config_dir))
    if not os.path.isdir(self.config_dir):
      parser.error("Configuration file directory %s does not exist or is not a directory" % self.config_dir)
    if options.log_dir:
      self.log_dir = os.path.abspath(os.path.expanduser(options.log_dir))
      if not os.path.isdir(self.log_dir):
        try:
          os.makedirs(self.log_dir)
        except:
          parser.error("Log directory %s does not exist and attempt at creating it failed" % self.log_dir)
    else: # log_dir was not specified, use stdout
      self.log_dir = None

    logger.info("Caretaker starting, BLOXPATH=%s, using_engage=%s" %
                (self.bloxpath, using_engage))
    if not using_engage:
      self.start_fileserver()
        
  def run(self):
    context = zmq.Context()
    self.socket = context.socket(zmq.REP)
    self.socket.bind('tcp://*:5000')
    os.system("rm %s/*.json" % self.config_dir)
    os.putenv("LC_CTYPE", "en_US.UTF-8")
    logger.info("Set encoding to en_US.UTF-8")
    logger.info('Care taker loaded')
    while True:
      try:
        message = self.socket.recv()
        control_data = json.loads(message)
        logger.info("[caretaker] received msg: " + message)
        control, data = control_data
        if control == "ADD BLOCK":
          res = self.start_block(data)
          self.socket.send(json.dumps(res))
        elif control == "POLL":
          res = self.collect_poll_data()
          self.socket.send(json.dumps(res))
        elif control == "STOP ALL":
          self.stop_all()
          self.processes = {}
          self.socket.send(json.dumps(True))
        elif control == "END RUN":
          self.stop_all()
          self.processes = {}
          self.socket.send(json.dumps(True))
        else:
          logger.info("[caretaker] **Warning could not understand master")
      except KeyboardInterrupt:
        logger.info("[caretaker] Stopping care_taker")
        break
          
    self.shutdown()

def call_from_console_script():
  c = CareTaker(sys.argv[1:])
  c.run()

if __name__ == "__main__":
  c = CareTaker(sys.argv[1:])
  c.run()
