import os
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from Crypto.Cipher import DES
import urllib

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
  with open(os.path.expanduser('~/datablox_file_server_key'), 'r') as f:
    deskey = f.read()

  MyServer.serve_forever(4990)