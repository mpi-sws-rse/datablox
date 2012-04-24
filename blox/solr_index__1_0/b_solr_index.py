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
    self.pending_entries = []
    self.num_tokens = config["crawlers"]
    self.port = config["port"] if config.has_key("port") else 8983
    self.indexer = sunburnt.SolrInterface("http://localhost:" + str(self.port) + "/solr/")
    self.log(INFO, "Solr-index block loaded")
  
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, self.id + " got the finish token for the directory " + log.log["token"][0])
      self.num_tokens = self.num_tokens - 1
      if self.num_tokens == 0:
        self.crawler_done = True
        self.add_pending_entries()
        self.indexer.commit()
        self.process_outstanding_queries()
    else: 
      self.index_entries(log)
  
  def add_pending_entries(self):
    self.log(INFO, "adding num entries: %d" % len(self.pending_entries))
    try:
      self.indexer.add(self.pending_entries)
    except Exception, e:
        self.log(ERROR, "Failed to add doc due to: %r" % (e))
        #raise
    self.pending_entries = []
    
  def index_entries(self, log):
    #TODO: hardcoded 500
    if len(self.pending_entries) > 500:
      self.add_pending_entries()
    for path, url in log.iter_fields("path", "url"):
      # self.log(INFO, "adding " + path)
      contents = BlockUtils.fetch_file_at_url(url)
      contents = contents.decode('utf-8', 'ignore')
      entry = {"path": path,
               "name": os.path.split(path)[-1],
               "contents": contents}
      self.pending_entries.append(entry)

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