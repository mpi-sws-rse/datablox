from block import *
from perf_counter import PerfCounter
import time
import sys, os, threading, time
from datetime import datetime
import base64
from logging import ERROR, WARN, INFO, DEBUG

# number of messages to process in a solr batch
BATCH_SIZE = 25

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
    indexer_url = "http://localhost:" + str(self.port) + "/solr/"
    self.log(INFO, "Indexer connecting to %s" % indexer_url)
    self.indexer = None
    tries = 0
    while not self.indexer:
      try:
        tries += 1
        self.indexer = sunburnt.SolrInterface(indexer_url)
      except Exception, e:
        self.logger.exception("Error in connecting to indexer: %s" % e)
        if tries == 10:
          self.logger.error("Giving up after 10 tries")
          raise
        else:
          time.sleep(2)
    self.log(DEBUG, "Connect to indexer successful")
    self.msg_timer = PerfCounter(self.name, "msgs")
    self.url_timer = PerfCounter(self.name, "url")
    self.log(INFO, "Solr-index block loaded")
  
  def recv_push(self, port, log):
    self.msg_timer.start_timer()
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
    self.msg_timer.stop_timer(log.num_rows())
  
  def add_pending_entries(self):
    self.log(INFO, "adding num entries: %d" % len(self.pending_entries))
    try:
      self.indexer.add(self.pending_entries)
    except Exception, e:
        self.log(ERROR, "Failed to add doc due to: %r" % (e))
        #raise
    self.pending_entries = []
    
  def index_entries(self, log):
    if len(self.pending_entries) > BATCH_SIZE:
      self.add_pending_entries()
    for path, url in log.iter_fields("path", "url"):
      self.url_timer.start_timer()
      contents = BlockUtils.fetch_file_at_url(url, self.ip_address)
      self.url_timer.stop_timer()
      contents = contents.decode('utf-8', 'ignore')
      entry = {"path": path,
               "name": os.path.split(path)[-1],
               "contents": contents}
      self.pending_entries.append(entry)

  def on_shutdown(self):
    self.indexer.commit()
    self.msg_timer.log_final_results(self.logger)
    self.url_timer.log_final_results(self.logger)
    
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
