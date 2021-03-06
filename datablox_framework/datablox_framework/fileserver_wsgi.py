
"""this is a version of the fileserver that works with wsgi (eg. gunicorn)"""

import os
import os.path
import urllib
import urlparse
import sys
import logging
from random import choice, randint
import string
import fcntl

logger = logging.getLogger("gunicorn.error")

DEBUG=True
# if we're debugging stuff, we log stack traces, otherwise we only log the error message
if DEBUG:
  log_exc=logger.exception
else:
  log_exc=logger.error

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

from block import decrypt_path

FILESERVER_PORT=4990

BLOCK_SIZE = 128000

KEY_MESSAGE = "key="
KEY_MESSAGE_LEN = len(KEY_MESSAGE)

def gen_random(length, chars=string.letters+string.digits):
    return ''.join([ choice(chars) for i in range(length) ])


def get_key(path):
  """Get the key for the fileserver. Since there could be
  multiple slaves, we need to use file locking to serialize
  access to the key file. The first slave to try to get the key
  will generate the key and create the file.
  """
  path = os.path.abspath(os.path.expanduser(path))
  lockfile = path + ".lock"
  with open(lockfile, "w") as lf:
    fcntl.lockf(lf, fcntl.LOCK_EX)
    try:
      if not os.path.exists(path):
        k = gen_random(8)
        with open(path, 'w') as f:
          f.write(k)
        os.chmod(path, 0400)
        logger.info("Generated new keyfile at %s" % path)
        return k
      else:
        with open(path, "r") as f:
          logger.info("Reading existing keyfile at %s" % path)
          return f.read().rstrip()
    finally:
      fcntl.lockf(lf, fcntl.LOCK_UN)

deskey = get_key(file_server_keypath)

error_headers = [("content-type", "text/plain")]

def send_file(path, size):
  with open(path) as f:
    block = f.read(BLOCK_SIZE)
    while block:
      yield block
      block = f.read(BLOCK_SIZE)
            
def app(environ, start_response):
  path = None
  try:
    qs = environ.get("QUERY_STRING")
    qdict = urlparse.parse_qs(qs)
    enc_path = qdict["key"][0]
    path = decrypt_path(enc_path, deskey)
    logger.debug("Decrypted path " + path)
    size = os.path.getsize(path)
  except KeyError, e:
    log_exc("Invalid request(KeyError): %s, query string was '%s'" %
            (e, qs))
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Invalid request"]
  except ValueError, e:
    log_exc("Invalid request (ValueError): %s, query string was '%s'" %
            (e, qs))
    if path:
      logger.error("Path was %s" % path)
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Invalid request"]
  except IOError:
    log_exc("Could not open file at %s" % path)
    start_response('404 Page Not Found', error_headers, sys.exc_info())
    return ["Could not open file at %s" % path]
  except Exception, e:
    log_exc("Unexpected error %s, query string was '%s'" % (e, qs))
    if path:
      logger.error("Path was %s" % path)
    start_response('500 Internal Server Error', error_headers, sys.exc_info())
    return ["Unexpected error %s" % e]
  start_response("200 OK", [
    ("Content-Length", str(size))
  ])
  return send_file(path, size)
    
