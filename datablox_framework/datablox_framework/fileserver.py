import os
import os.path
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from Crypto.Cipher import DES
import urllib

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

deskey = None

class MyServer(BaseHTTPRequestHandler):
  def do_GET(self):
    global deskey
    
    print self.path
    try:
      key_message = "/?key="
      loc = self.path.index(key_message)
      print "str type is ", type(self.path[loc + len(key_message):])
      enc_path = urllib.unquote(self.path[loc + len(key_message):])
      obj = DES.new(deskey, DES.MODE_ECB)
      path = obj.decrypt(enc_path)
      print "Decrypted path " + path
      with open(path, 'r') as f:
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(f.read())
    except ValueError:
      print "Invalid request"
      self.send_response(404, 'Page Not Found')
    except IOError:
      print "Could not open file"
      self.send_response(404, 'Page Not Found')

  @staticmethod
  def serve_forever(port):
    HTTPServer(('', port), MyServer).serve_forever()

if __name__ == "__main__":
  with open(file_server_keypath, 'r') as f:
    deskey = f.read()

  MyServer.serve_forever(4990)
