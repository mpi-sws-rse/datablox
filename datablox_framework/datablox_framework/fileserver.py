import os
import os.path
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from Crypto.Cipher import DES
import urllib
import urlparse
import sys
from optparse import OptionParser
import logging
from random import choice, randint
import string
import base64

logger = logging.getLogger(__name__)

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

deskey = None

def decrypt_path(encoded_path, key):
  # The encoded_path is encrypted and base64 encoded. Base64 decoding
  # doesn't seem to work on unicode strings (which is what we get back when
  # parsing the path from the url). We encode as ascii first (the base64 encoding
  # only uses valid ascii characters).
  encoded_path = encoded_path.encode("ascii")
  encrypted_path = base64.urlsafe_b64decode(encoded_path)
  obj = DES.new(key, DES.MODE_ECB)
  path = unicode(obj.decrypt(encrypted_path), encoding="utf-8")
  return path

def gen_random(length, chars=string.letters+string.digits):
    return ''.join([ choice(chars) for i in range(length) ])

class MyServer(BaseHTTPRequestHandler):
  def do_GET(self):
    global deskey
    
    logger.info(self.path)
    try:
      qdict = urlparse.parse_qs(self.path)
      enc_path = qdict["key"][0]
      obj = DES.new(deskey, DES.MODE_ECB)
      path = decrypt_path(enc_path, deskey)
      logger.info("Decrypted path " + path)
      with open(path, 'r') as f:
        self.send_response(200, 'OK')
        #self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(f.read())
    except ValueError:
      logger.error("Invalid request")
      self.send_response(404, 'Page Not Found')
    except KeyError:
      logger.error("Invalid request, no parameter named 'key'")
      self.send_response(404, 'Page Not Found')
    except IOError:
      logger.error("Could not open file")
      self.send_response(404, 'Page Not Found')
    except Exception, e:
      logger.exception("Unexpected error %s" % e)
      self.send_response(500, 'Internal Server Error')

  @staticmethod
  def serve_forever(port):
    HTTPServer(('', port), MyServer).serve_forever()

def main(argv=sys.argv[1:]):
  global deskey
  parser = OptionParser()
  parser.add_option("--config-dir", dest="config_dir", default=None,
                    help="directory for key file and other fileserver configuration")

  (options, args) = parser.parse_args(argv)
  if options.config_dir and (not os.path.exists(options.config_dir)):
    parser.error("Configuration directory %s does not exist" % options.config_dir)
  
  root_logger = logging.getLogger()
  if len(root_logger.handlers)==0:
    console_handler = logging.StreamHandler(sys.stdout)
    if using_engage:
      log_level = logging.DEBUG # stdout is going to a file anyway
    else:
      log_level = logging.INFO
    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(log_level)

  deskey = gen_random(8)
  with open(file_server_keypath, 'w') as f:
    f.write(deskey)

  logger.info("Starting server on port %d" % FILESERVER_PORT)
  MyServer.serve_forever(FILESERVER_PORT)


if __name__ == "__main__":
  main()
