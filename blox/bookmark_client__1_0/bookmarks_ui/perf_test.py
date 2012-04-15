import xmlrpclib
import json
import cgi
import time
<<<<<<< HEAD

proxy = xmlrpclib.ServerProxy("http://localhost:8000/")

urls = ["http://www.google.com", "http://www.python.org/", "http://www.msn.com", 
        "http://www.ucla.edu", "http://www.cs.ucla.edu", "http://geekandpoke.typepad.com/", 
        "http://www.apple.com", "https://github.com/", "http://www.microsoft.com",
        "http://www.daringfireball.net"]
=======
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

# urls = ["http://www.google.com", "http://www.python.org/", "http://www.msn.com", 
#         "http://www.ucla.edu", "http://www.cs.ucla.edu", "http://geekandpoke.typepad.com/", 
#         "http://www.apple.com", "https://github.com/", "http://www.microsoft.com",
#         "http://www.daringfireball.net"]
>>>>>>> master

print "start time: %r" % (time.localtime())

start = time.time()

for url in urls:
  print "url: %r, time: %r" % (url, time.localtime())
  log = {"internet_url": [url]}
  res = json.loads(proxy.bookmark(json.dumps(log)))["result"]
  assert(res == True)

duration = time.time() - start
print "Total adding time: ", duration