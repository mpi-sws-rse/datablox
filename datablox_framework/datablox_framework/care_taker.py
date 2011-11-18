import zmq
import json
import os

def main():
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
        command += os.environ["BLOXPATH"]
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
  main()