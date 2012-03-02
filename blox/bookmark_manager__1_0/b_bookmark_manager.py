from block import *
import collections
from collections import defaultdict
import time
from logging import ERROR, WARN, INFO, DEBUG

class bookmark_manager(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["internet_url", "url", "fingerprint"])
    self.add_port("store", Port.PUSH, Port.UNNAMED, ["url", "fingerprint"])
    self.add_port("meta_store", Port.PUSH, Port.UNNAMED, ["path", "fingerprints"])
  
  def add_meta(self, log):
    related = defaultdict(list)
    for rel, iu, fp in log.iter_fields("related_to", "internet_url", "fingerprint"):
      related[rel].append((iu, fp))
    mlog = Log()
    paths = related.keys()
    mlog.append_field("path", paths)
    #TODO: Using local time now, maybe use GMT
    mlog.append_field("time", [time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.localtime()) for t in paths])
    related_assets = [related[iurl] for iurl in paths]
    mlog.append_field("assets", related_assets)
    self.push("meta_store", mlog)

  def add_chunks(self, log):
    clog = Log()
    clog.append_field("url", log.log["url"])
    clog.append_field("fingerprint", log.log["fingerprint"])
    self.push("store", clog)

  def recv_push(self, port, log):
    self.add_chunks(log)
    self.add_meta(log)
    self.log(INFO, "perf: done adding URL at %r" % time.localtime())