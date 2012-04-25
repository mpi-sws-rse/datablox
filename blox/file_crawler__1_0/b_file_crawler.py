from block import *
import time
import os
import os.path
import socket
import base64
from logging import ERROR, WARN, INFO, DEBUG
import time

# file logging will only show up if we set the log level to ALL
FILE_LOGGING=DEBUG-1
LOG_SIZE_LIMIT=50


try:
  HOST = socket.gethostname()
except:
  HOST = "local"

def get_volume_name(volume_path):
  return HOST + ":" + volume_path

class file_crawler(Block):
  def add_file(self, host, volume, path, stat):
    listing = {}
    listing["path"] = host + ":" + path
    listing["size"] = stat.st_size
    listing["perm"] = stat.st_mode
    listing["owner"] = stat.st_uid
    listing["volume"] = volume
    listing["url"] = BlockUtils.generate_url_for_path(path)
    listing["crawl_id"] = self.crawl_id
    
    self.current_log.append_row(listing)

  def send_log(self):
    if self.current_log.num_rows()>0:
      self.buffered_push("output", self.current_log)
      self.current_log = Log()

  @benchmark
  def send_token(self, volume_name):
    self.send_log()
    token = {"token": [volume_name]}
    log = Log()
    log.set_log(token)
    self.buffered_push("output", log)
    self.flush_port("output")
    
  def do_task(self):
    files_sent_in_session = {"cnt":0}
    # Function to actually handle the file.
    # We keep it as a separate function to enable the @benchmark
    # decorator (defined in block.py).
    @benchmark
    def process_file(self, fpath):
      try:
        stat = os.stat(fpath)
        self.log(FILE_LOGGING, "Sending file %s" % fpath)
        self.add_file(HOST, volume_name, fpath, stat)
        if self.current_log.num_rows()>=LOG_SIZE_LIMIT:
          self.send_log()
        files_sent_in_session["cnt"] += 1
        if files_sent_in_session["cnt"] > self.single_session_limit:
          files_sent_in_session["cnt"] = 0
          self.send_log()
          return False
      except OSError:
        self.log(WARN, "not dealing with file " + fpath)
      return True
    # just a function to use for wrapping by the print_benchmarks decorator
    @print_benchmarks
    def done(self):
      pass
    path = os.path.abspath(os.path.expanduser(self.config["directory"]))
    volume_name = get_volume_name(path)
    self.log(INFO, "Starting walk at %s" % volume_name)
    # the main loop
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        fpath = os.path.join(root, filename)
        within_session_limit = process_file(self, fpath)
        if not within_session_limit:
          yield
    yield
    #this will clear all outstanding files in the buffer
    self.send_token(volume_name)
    done(self)

  def on_load(self, config):
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner" ,"volume", "url"])
    self.single_session_limit = 50
    if config.has_key("buffer_limit"):
      self.buffer_limit = config["buffer_limit"]
    if config.has_key("crawl_id"):
      #if you put a crawl_id in the config file, it is your responsibility to
      #ensure that it is unique.
      self.crawl_id = unicode(config["crawl_id"])
    else:
      self.crawl_id = self.id + " " + time.ctime()
    self.log(INFO, "File-Crawler crawl_id = %s" % self.crawl_id)
    self.log(INFO, "File-Crawler block loaded")
    self.current_log = Log()


