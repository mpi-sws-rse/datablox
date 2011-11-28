import zmq
import json
import os
import sys
from optparse import OptionParser

def main(argv):
  usage = "%prog [options]"
  parser = OptionParser(usage=usage)
  parser.add_option("-b", "--bloxpath", dest="bloxpath", default=None,
                    help="use this path instead of the environment variable BLOXPATH")
                    
  (options, args) = parser.parse_args(argv)

  bloxpath = options.bloxpath
  
  if bloxpath == None: 
    if not os.environ.has_key("BLOXPATH"):
      parser.error("Need to set BLOXPATH environment variable or pass it as an argument")
    else:
      bloxpath = os.environ["BLOXPATH"]

  if not os.path.isdir(bloxpath):
    parser.error("BLOXPATH %s does not exist or is not a directory" % bloxpath)
  
  context = zmq.Context()
  socket = context.socket(zmq.REP)
  socket.bind('tcp://*:5000')
  os.system("rm *.json")
  file_num = 0
  while True:
    try:
      message = socket.recv()
      control_data = json.loads(message)
      print control_data
      control, data = control_data
      if control == "ADD NODE":
        config_name = data["name"] + str(file_num) + ".json"
        file_num += 1
        with open(config_name, 'w') as config_file:
          json.dump(data, config_file)
        command = "python load_block.py "
        command += bloxpath
        command += " " + config_name
        command += " &"
        os.system(command)
        socket.send(json.dumps(True))
      else:
        print "**Warning could not understand master"
    except KeyboardInterrupt:
      print "Stopping care_taker"
      break

if __name__ == "__main__":
  main(sys.argv[1:])