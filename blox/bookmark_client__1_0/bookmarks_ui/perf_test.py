import xmlrpclib
import json
import cgi
import time

proxy = xmlrpclib.ServerProxy("http://localhost:8000/")

urls = ["http://www.google.com", "http://www.python.org/", "http://www.msn.com", 
        "http://www.ucla.edu", "http://www.cs.ucla.edu", "http://geekandpoke.typepad.com/", 
        "http://www.apple.com", "https://github.com/", "http://www.microsoft.com",
        "http://www.daringfireball.net"]

for url in urls:
  print "url: %r, time: %r" % (url, time.localtime())
  log = {"internet_url": [url]}
  res = json.loads(proxy.bookmark(json.dumps(log)))["result"]
  assert(res == True)