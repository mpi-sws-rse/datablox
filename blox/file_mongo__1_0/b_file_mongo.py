from logging import ERROR, WARN, INFO, DEBUG
import time

from block import *

class file_mongo(Block):
  def __init__(self, master_url):
    Block.__init__(self, master_url)
    self.volumes_processed = []
    
  def on_load(self, config):
    import pymongo
    from pymongo import Connection
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner"])
    self.add_port("file_data", Port.QUERY, Port.UNNAMED, ["name"])
    self.add_port("dir_aggregates", Port.QUERY, Port.UNNAMED, ["name"])
    self.add_port("file_duplicates", Port.QUERY, Port.UNNAMED, [])
    self.add_port("completed", Port.PUSH, Port.UNNAMED, ["key"])
    self.connection = Connection()
    db = self.connection['file_db']
    self.file_data = db.file_data
    self.crawler_done = False
    self.num_tokens = config["crawlers"]
    self.queries = []
    self.log(INFO, "File-mongo block loaded")

  def emit_completed_message(self, volume, is_last):
    self.volumes_processed.append(volume)
    log = Log()
    log.set_log({"key": volume})
    self.push("completed", log)
    if is_last:
      log = Log()
      log.set_log({"token":self.volumes_processed})
      self.push("completed", log)
      self.log(INFO, "emitted finish token: %s" % self.volumes_processed)
    
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, "got the finish token for directory " + log.log["token"][0])
      self.num_tokens = self.num_tokens - 1
      self.emit_completed_message(log.log["token"][0], self.num_tokens==0)
      if self.num_tokens == 0:
        self.crawler_done = True
        self.process_outstanding_queries()
    else:
      entries = []
      if log.log.has_key("url"):
        log.remove_field("url")
      cnt = 0
      for l in log.iter_flatten():
        cnt += 1
        # self.file_data.remove({"path" : l["path"]})
        entries.append(l)
      stime = time.time()
      self.file_data.insert(entries)
      etime = time.time()
      self.log(DEBUG, "Insert of %d records took %.3f seconds" %
               (cnt, (etime - stime)))

  def recv_query(self, port_name, log):
    if not self.crawler_done:
      self.log(DEBUG, "got a query request, but waiting for crawler to be done")
      self.queries.append((port_name, log))
    else:
      self.process_query(port_name, log)
  
  def process_outstanding_queries(self):
    assert(self.crawler_done == True)
    for q in self.queries:
      self.process_query(q[0], q[1])
    
  def process_query(self, port_name, log):
    entry = {}
    if port_name == "file_data":
      paths = log.log["paths"]
      names = []
      sizes = []
      perms = []
      owners = []
      categories = []
      for path in paths:
        fd = self.file_data.find_one({"path": path})
        names.append(fd["name"])
        sizes.append(fd["size"])
        perms.append(fd["perm"])
        owners.append(fd["owner"])
        categories.append(fd["category"])
      entry["name"] = names
      entry["path"] = paths
      entry["size"] = sizes
      entry["perm"] = perms
      entry["owner"] = owners
      entry["category"] = categories
    elif port_name == "dir_aggregates":
      total_size = 0.0
      num_files = 0
      for fd in self.file_data.find():
        num_files = num_files + 1
        total_size = total_size + fd["size"]
      avg_size = total_size / num_files
      entry = {"total_size": total_size,
               "num_files": num_files,
               "avg_size": avg_size
              }
      self.log(DEBUG, "returning aggregates")
    elif port_name == "file_duplicates":
      hc = self.file_data.distinct('hash')
      hashes = [c for c in hc]
      dupe_hashes = []
      dupe_files = []
      for hash in hashes:
        files = [f["path"] for f in self.file_data.find({'hash': hash})]
        if len(files) > 1:
          dupe_hashes.append(hash)
          dupe_files.append(files)
      entry["hash"] = dupe_hashes
      entry["files"] = dupe_files
    else:
      self.log(WARN, "Got a request from unknown port")
    
    ret_log = Log()
    ret_log.set_log(entry)
    self.return_query_res(port_name, ret_log)
  
  def on_shutdown(self):
    self.connection.disconnect()
