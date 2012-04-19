import subprocess
import sys

if len(sys.argv) != 5:
  print "%s: requires a script name, URL file name arguments, number of instances and number of URLs to distribute"
  sys.exit(1)

script = sys.argv[1]
url_file = sys.argv[2]
instances = int(sys.argv[3])
num_urls = int(sys.argv[4])

start_index = 0
distance = int(num_urls/instances)
print "Num urls per instance:", distance

while(instances > 0):
  end_index = start_index + distance if instances > 1 else num_urls
  if end_index > num_urls:
    end_index = num_urls
  command = [sys.executable, script, url_file, str(start_index), str(end_index)]
  subprocess.Popen(command)
  instances -= 1
  start_index += distance

sys.exit(0)