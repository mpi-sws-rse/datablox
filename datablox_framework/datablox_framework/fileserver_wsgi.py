
"""this is a version of the fileserver that works with wsgi (eg. gunicorn)"""

import os
import os.path
import urllib
import urlparse
import sys
import logging
from Crypto.Cipher import DES
from random import choice, randint
import string

logger = logging.getLogger("gunicorn.error")

try:
  import datablox_engage_adapter.file_locator
  using_engage = True
except ImportError:
  using_engage = False

if using_engage:
  engage_file_locator = datablox_engage_adapter.file_locator.FileLocator()
  file_server_keypath = engage_file_locator.get_file_server_key_file()
else:
  file_server_keypath = os.path.expanduser('~/datablox_file_server_key')

FILESERVER_PORT=4990

BLOCK_SIZE = 128000

KEY_MESSAGE = "key="
KEY_MESSAGE_LEN = len(KEY_MESSAGE)

def gen_random(length, chars=string.letters+string.digits):
    return ''.join([ choice(chars) for i in range(length) ])

# with open(file_server_keypath, "r") as f:
#     deskey = f.read()
deskey = gen_random(8)
with open(file_server_keypath, 'w') as f:
  f.write(deskey)

error_headers = [("content-type", "text/plain")]

def send_file(path, size):
  with open(path) as f:
    block = f.read(BLOCK_SIZE)
    while block:
      yield block
      block = f.read(BLOCK_SIZE)
            
def app(environ, start_response):
  try:
    qs = environ.get("QUERY_STRING")
    qdict = urlparse.parse_qs(qs)
    enc_path = qdict["key"][0]
    obj = DES.new(deskey, DES.MODE_ECB)
    path = obj.decrypt(enc_path)
    path = path.decode('utf-8')
    logger.debug("Decrypted path " + path)
    size = os.path.getsize(path)
  except KeyError, e:
    logger.error("Invalid request: %s" % e)
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Invalid request"]
  except ValueError, e:
    logger.error("Invalid request: %s" % e)
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Invalid request"]
  except IOError:
    logger.error("Could not open file at %s" % path)
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Could not open file at %s" % path]
  except Exception, e:
    logger.error("Unexpected error %s" % e)
    start_response('500 Internal Server Error', error_headers, sys.exc_info())
    return ["Unexpected error %s" % e]
  start_response("200 OK", [
    ("Content-Length", str(size))
  ])
  return send_file(path, size)
    
