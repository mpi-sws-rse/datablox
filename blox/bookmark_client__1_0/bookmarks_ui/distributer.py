import subprocess
from subprocess import PIPE
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

pipes = []

while(instances > 0):
  end_index = start_index + distance if instances > 1 else num_urls
  if end_index > num_urls:
    end_index = num_urls
  command = [sys.executable, script, url_file, str(start_index), str(end_index)]
  p1 = subprocess.Popen(command, stdout=PIPE)
  instances -= 1
  start_index += distance
  pipes.append(p1)

f = open("distributer_output", 'w')
for p in pipes:
    f.write(p.communicate()[0])

sys.exit(0)