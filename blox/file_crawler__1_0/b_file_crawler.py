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
    # helpers for managing timer. To improve accuracy, we time the
    # sessions and divide rather than measure individual messages.
    if not hasattr(self, "benchmark_dict"):
      self.benchmark_dict = {}
    d = self.benchmark_dict
    if not d.has_key("total_duration"):
      d["total_duration"] = 0
      d["num_calls"] = 0
    files_sent_in_session = 0
    def start_timer():
      assert files_sent_in_session==0
      assert not d.has_key("start_time")
      d["start_time"] = time.time()
    def stop_timer():
      duration = time.time() - d["start_time"]
      d["total_duration"] += duration
      d["num_calls"] += files_sent_in_session
      del d["start_time"]
    # just a function to use for wrapping by the print_benchmarks decorator
    @print_benchmarks
    def done(self):
      pass
    path = os.path.abspath(os.path.expanduser(self.config["directory"]))
    volume_name = get_volume_name(path)
    self.log(INFO, "Starting walk at %s" % volume_name)
    # the main loop
    start_timer()
    for root, dirnames, filenames in os.walk(path):
      for filename in filenames:
        fpath = os.path.join(root, filename)
        try:
          stat = os.stat(fpath)
          self.log(FILE_LOGGING, "Sending file %s" % fpath)
          self.add_file(HOST, volume_name, fpath, stat)
          if self.current_log.num_rows()>=LOG_SIZE_LIMIT:
            self.send_log()
          files_sent_in_session += 1
          if files_sent_in_session > self.single_session_limit:
            self.send_log()
            stop_timer()
            files_sent_in_session = 0
            yield
            start_timer()
        except OSError:
          self.log(WARN, "not dealing with file " + fpath)
    stop_timer()
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


