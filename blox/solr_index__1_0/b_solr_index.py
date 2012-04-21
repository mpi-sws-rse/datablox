from block import *
import time
import sys, os, threading, time
from datetime import datetime
import base64
from logging import ERROR, WARN, INFO, DEBUG

class solr_index(Block):
  def on_load(self, config):
    import sunburnt
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name"])
    self.add_port("query", Port.QUERY, Port.UNNAMED, ["query"])
    self.crawler_done = False
    self.queries = []
    self.num_tokens = config["crawlers"]
    self.indexer = sunburnt.SolrInterface("http://localhost:8983/solr/")
    self.log(INFO, "Solr-index block loaded")
  
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.id + " got the finish token for the directory " + log.log["token"][0])
      self.num_tokens = self.num_tokens - 1
      if self.num_tokens == 0:
        self.crawler_done = True
        self.indexer.commit()
        self.process_outstanding_queries()
    elif log.log.has_key("data"):
      self.index_entries(log.log)
    else:
      files = log.log["path"]
      try:
          self.index_docs(files)
      except Exception, e:
          self.log(ERROR, "Failed: ", e)
  
  def indexible_file(self, path):
    return path.endswith('.txt') or path.endswith('.py')
    
  def index_docs(self, paths):
    for path in paths:
      if not self.indexible_file(path):
        continue
      self.log(INFO, "adding", path)
      try:
        file = open(path)
        contents = unicode(file.read(), 'iso-8859-1')
        file.close()
        entry = {"path": path,
                "name": os.path.split(path)[-1],
                "contents": contents}
        self.indexer.add(entry)
      except Exception, e:
          self.log(ERROR, "Failed in index_docs:", e)
  
  def index_entries(self, log):
    paths = log["path"]
    data = log["data"]
    # self.log(INFO, "path len: %d, data len %d" % (len(paths), len(data)))
    for i in range(len(paths)):
      if not self.indexible_file(paths[i]):
        continue
      self.log(INFO, "adding " + paths[i])
      entry = {"path": paths[i],
               "name": os.path.split(paths[i])[-1],
               "contents": base64.b64decode(data[i])}
      try:
        self.indexer.add(entry)
      except Exception, e:
          self.log(ERROR, "Failed to add doc %s due to: %r" % (paths[i], e))
    
  def on_shutdown(self):
    self.indexer.commit()
    
  def recv_query(self, port_name, log):
    if not self.crawler_done:
      self.log(INFO, self.id + " got a query request, but waiting for crawler to be done")
      self.queries.append((port_name, log))
    else:
      self.process_query(port_name, log)
  
  def process_outstanding_queries(self):
    assert(self.crawler_done == True)
    for q in self.queries:
      self.process_query(q[0], q[1])

  def process_query(self, port_name, log):
    query = log.log["query"]
    res = self.indexer.query(query).execute()
    paths = [r["path"] for r in res]
    nl = Log()
    nl.log["results"] = paths
    self.return_query_res(port_name, nl)