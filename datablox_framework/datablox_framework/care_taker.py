import zmq
import json
import os
import os.path
import sys
import subprocess
from optparse import OptionParser

def stop_all(proccesses):
  print "[caretaker] stopping all blocks"
  for p in proccesses:
    p.terminate()
  print "[caretaker] done"
  
def main(argv):
  usage = "%prog [options]"
  parser = OptionParser(usage=usage)
  parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                    help="use this path instead of the environment variable BLOXPATH")
  parser.add_option("--config-dir", dest="config_dir", default=".",
                    help="directory to use for storing configuration files for the individual blocks")
  parser.add_option("--log-dir", dest="log_dir", default=None,
                     help="Directory to use for log files, if not specified just use the console")

  (options, args) = parser.parse_args(argv)

  bloxpath = options.bloxpath
  
  if bloxpath == None: 
    if not os.environ.has_key("BLOXPATH"):
      parser.error("Need to set BLOXPATH environment variable or pass it as an argument")
    else:
      bloxpath = os.environ["BLOXPATH"]

  if not os.path.isdir(bloxpath):
    parser.error("BLOXPATH %s does not exist or is not a directory" % bloxpath)

  config_dir = os.path.abspath(os.path.expanduser(options.config_dir))
  if not os.path.isdir(config_dir):
    parser.error("Configuration file directory %s does not exist or is not a directory" % config_dir)
  if options.log_dir:
    log_dir = os.path.abspath(os.path.expanduser(options.log_dir))
    if not os.path.isdir(log_dir):
      try:
        os.makedirs(log_dir)
      except:
        parser.error("Log directory %s does not exist and attempt at creating it failed" % log_dir)
  else: # log_dir was not specified, use stdout
    log_dir = None
  context = zmq.Context()
  socket = context.socket(zmq.REP)
  socket.bind('tcp://*:5000')
  os.system("rm %s/*.json" % config_dir)
  file_num = 0
  proccesses = []
  while True:
    try:
      message = socket.recv()
      control_data = json.loads(message)
      print "[caretaker] received msg: " + message
      control, data = control_data
      if control == "ADD NODE":
        config_name = os.path.join(config_dir,
                                   data["name"] + str(file_num) + ".json")
        file_num += 1
        with open(config_name, 'w') as config_file:
          json.dump(data, config_file)
        load_block_script = os.path.join(os.path.dirname(__file__),
                                         "load_block.py")
        command = [sys.executable, load_block_script, bloxpath, config_name]
        if log_dir:
          command.append(log_dir)
        p = subprocess.Popen(command)
        proccesses.append(p)
        socket.send(json.dumps(True))
      elif control == "STOP ALL":
        stop_all(proccesses)
        proccesses = []
        socket.send(json.dumps(True))
      else:
        print "[caretaker] **Warning could not understand master"
    except KeyboardInterrupt:
      print "[caretaker] Stopping care_taker"
      stop_all(proccesses)
      proccesses = []
      break

def call_from_console_script():
    sys.exit(main(sys.argv[1:]))

if __name__ == "__main__":
  main(sys.argv[1:])
