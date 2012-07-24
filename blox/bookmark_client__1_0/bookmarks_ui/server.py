#!/usr/bin/env python

import CGIHTTPServer
import BaseHTTPServer
from optparse import OptionParser
import os
import os.path

class Handler(CGIHTTPServer.CGIHTTPRequestHandler):
    cgi_directories = ["/www"]

PORT = 9000

parser = OptionParser()
parser.add_option("--pidfile", dest="pidfile", default=None,
                  help="write out pid to the specified file")
(options, args) = parser.parse_args()
        
httpd = BaseHTTPServer.HTTPServer(("", PORT), Handler)
print "serving at port", PORT
if options.pidfile:
    pidfilename = os.path.abspath(os.path.expanduser(options.pidfile))
    with open(pidfilename, "w") as f:
        f.write("%d" % os.getpid())
        print "wrote pid %d to %s" % (os.getpid(), pidfilename)
httpd.serve_forever()
