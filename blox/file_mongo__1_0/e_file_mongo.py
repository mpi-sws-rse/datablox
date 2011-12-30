from element import *

class file_mongo(Element):
  def on_load(self, config):
    import pymongo
    from pymongo import Connection
    self.name = "File-mongo"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name", "size", "perm", "owner"])
    self.add_port("file_data", Port.PULL, Port.UNNAMED, ["name"])
    self.add_port("dir_aggregates", Port.PULL, Port.UNNAMED, ["name"])
    self.add_port("file_duplicates", Port.PULL, Port.UNNAMED, [])
    print "File-mongo element loaded"
    self.connection = Connection()
    db = self.connection['file_db']
    self.file_data = db.file_data
    self.crawler_done = False
    self.num_tokens = config["crawlers"]
    self.queries = []

  def recv_push(self, port, log):
    if log.log.has_key("token"):
      print self.name + " got the finish token for directory " + log.log["token"]
      self.num_tokens = self.num_tokens - 1
      if self.num_tokens == 0:
        self.crawler_done = True
        self.process_outstanding_queries()
    else:
      entries = []
      if log.log.has_key("data"):
        log.remove_field("data")
      for l in log.iter_flatten():
        # print self.name + " adding entry " + str(l)
        self.file_data.remove({"path" : l["path"]})
        entries.append(l)
      self.file_data.insert(entries)

  def recv_pull_query(self, port_name, log):
    if not self.crawler_done:
      print self.name + " got a pull request, but waiting for crawler to be done"
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
      print "returning aggregates"
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
      print "Got a request from unknown port"
    
    ret_log = Log()
    ret_log.set_log(entry)
    self.return_pull(port_name, ret_log)
  
  def on_shutdown(self):
    self.connection.disconnect()
