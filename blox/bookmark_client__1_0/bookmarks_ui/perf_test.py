import xmlrpclib
import json
import cgi
import time
import sys

if len(sys.argv) != 4:
  print "%s: requires a file name and two indices as arguments"
  sys.exit(1)

proxy = xmlrpclib.ServerProxy("http://localhost:8000/")
with open(sys.argv[1]) as f:
  urls = f.readlines()

start_index = int(sys.argv[2])
end_index = int(sys.argv[3])
urls = urls[start_index: end_index]
urls = [u.strip() for u in urls]

print "Downloading %d urls" % (len(urls))

print "start time: %r" % (time.ctime())

start = time.time()

for url in urls:
  print "url: %r, time: %r" % (url, time.ctime())
  log = {"internet_url": [url]}
  res = json.loads(proxy.bookmark(json.dumps(log)))["result"]
  assert(res == True)

duration = time.time() - start
print "Total adding time: ", duration
proxy.shutdown()