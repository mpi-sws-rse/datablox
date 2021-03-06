from block import *
import time
import os
import os.path
import socket
import base64
from logging import ERROR, WARN, INFO, DEBUG
import time

from perf_counter import PerfCounter

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
    listing["path"] = unicode(host) + u":" + path
    listing["size"] = stat.st_size
    listing["perm"] = stat.st_mode
    listing["owner"] = stat.st_uid
    listing["volume"] = volume
    try:
      listing["url"] = BlockUtils.generate_url_for_path(path, self.ip_address)
      self.logger.log(FILE_LOGGING, "Url for path '%s' is '%s'" % (path, listing["url"]))
    except Exception, e:
      self.logger.error("Generate url for path %s failed: %s" % (path, e))
      raise
    listing["crawl_id"] = self.crawl_id
    
    self.current_log.append_row(listing)

  def send_log(self):
    if self.current_log.num_rows()>0:
      self.buffered_push("output", self.current_log)
      self.current_log = Log()

  def send_token(self, volume_name):
    self.msg_timer.start_timer()
    self.send_log()
    token = {"token": [volume_name]}
    log = Log()
    log.set_log(token)
    self.buffered_push("output", log)
    self.flush_port("output")
    self.msg_timer.stop_timer()
    
  def do_task(self):
    files_sent_in_session = 0
    path = os.path.abspath(os.path.expanduser(unicode(self.config["directory"])))
    if self.volume_name:
      volume_name = self.volume_name
    else:
      volume_name = get_volume_name(path)
    self.log(INFO, "Starting walk at %s" % volume_name)
    # the main loop
    self.msg_timer.start_timer()
    for root, dirnames, filenames in os.walk(path):
      root = unicode(root)
      for filename in filenames:
        fpath = os.path.join(root, unicode(filename))
        try:
          stat = os.stat(fpath)
          self.log(FILE_LOGGING, "Sending file %s" % fpath)
          self.add_file(HOST, volume_name, fpath, stat)
          if self.current_log.num_rows()>=LOG_SIZE_LIMIT:
            self.send_log()
          files_sent_in_session += 1
          if files_sent_in_session > self.single_session_limit:
            self.send_log()
            self.msg_timer.stop_timer(files_sent_in_session)
            files_sent_in_session = 0
            yield
            self.msg_timer.start_timer()
        except OSError:
          self.log(WARN, "not dealing with file " + fpath)
          self.errors += 1
          check_if_error_threshold_reached(self, self.errors, self.msg_timer.num_events)
    self.msg_timer.stop_timer(self.current_log.num_rows())
    yield
    #this will clear all outstanding files in the buffer
    self.send_token(volume_name)
    self.msg_timer.log_final_results(self.logger)
    self.log(INFO, "Total errors: %d" % self.errors)

  def on_load(self, config):
    self.config = config
    assert os.environ.has_key("LC_CTYPE"), \
      "Missing LC_CTYPE environment variable, which is needed for setting the character set"
    assert os.environ["LC_CTYPE"].endswith("UTF-8"), \
        "environment variable LC_CTYPE is %s, but should end with UTF-8 (e.g. en_US.UTF-8)" % \
        os.environ["LC_CTYPE"]
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
    if self.config.has_key("volume_name"):
      self.volume_name = self.config["volume_name"]
    else:
      self.volume_name = None
    self.errors = 0
    self.max_error_pct = config["max_error_pct"] if config.has_key("max_error_pct") \
                         else 10.0
    self.log(INFO, "File-Crawler crawl_id = %s" % self.crawl_id)
    self.log(INFO, "File-Crawler block loaded")
    self.current_log = Log()
    self.msg_timer = PerfCounter(self.block_name, "msgs")


