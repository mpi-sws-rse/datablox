from block import *
from perf_counter import PerfCounter
import time
import sys, os, threading, time
from datetime import datetime
import base64
from logging import ERROR, WARN, INFO, DEBUG

# number of messages to process in a solr batch
BATCH_SIZE = 5

class solr_index(Block):
  def on_load(self, config):
    import sunburnt
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name"])
    self.add_port("query", Port.QUERY, Port.UNNAMED, ["query"])
    self.crawler_done = False
    self.queries = []
    self.pending_entries = []
    self.num_tokens = config["crawlers"]
    self.max_error_pct = config["max_error_pct"] if config.has_key("max_error_pct") \
                         else 10.0
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
    self.msg_timer = PerfCounter(self.block_name, "msgs")
    self.url_timer = PerfCounter(self.block_name, "url")
    self.bytes_processed = 0
    self.errors = 0
    self.log(INFO, "Solr-index block loaded")
  
  def _commit(self):
    try:
      self.indexer.commit()
    except Exception, e:
      self.logger.exception("Indexer commit failed with exception %s" % e)
      self.errors += 1 # we don't really know how many documents this will affect
      raise

  def recv_push(self, port, log):
    self.msg_timer.start_timer()
    if log.log.has_key("token"):
      self.log(INFO, self.id + " got the finish token for the directory " + log.log["token"][0])
      self.num_tokens = self.num_tokens - 1
      if self.num_tokens == 0:
        self.crawler_done = True
        self.add_pending_entries()
        self._commit()
        self.process_outstanding_queries()
    else: 
      try:
        self.index_entries(log)
      except TooManyErrors, e:
        self.logger.error("%s" % e)
        raise
      except Exception, e:
        self.logger.exception("index_entries failed: %s" % e)
        self.errors += log.num_rows()
    self.msg_timer.stop_timer(log.num_rows())
  
  def add_pending_entries(self):
    self.log(INFO, "adding num entries: %d" % len(self.pending_entries))
    try:
      self.indexer.add(self.pending_entries)
    except Exception, e:
        self.log(ERROR, "Failed to add doc due to: %r" % (e))
        self.errors += 1
        check_if_error_threshold_reached(self, self.errors, self.url_timer.num_events)
    self.pending_entries = []
    
  def index_entries(self, log):
    if len(self.pending_entries) > BATCH_SIZE:
      self.add_pending_entries()
    for path, url in log.iter_fields("path", "url"):
      self.url_timer.start_timer()
      try:
        (contents, expected_len) = BlockUtils.fetch_file_at_url(url, self.ip_address, check_size=True)
      except Exception, e:
        # we might get an error because we cannot read the file
        self.logger.exception("Got exception '%s' when trying to access url '%s'"
                              % (e, url))
        self.logger.error("The associated path was '%s'" % path)
        self.errors += 1
        contents = None
        check_if_error_threshold_reached(self, self.errors, self.url_timer.num_events)
      self.url_timer.stop_timer()
      if contents:
        phys_len = len(contents)
        if expected_len != phys_len:
          self.logger.error("Length mismatch in file %s: expecting %ld, got %ld" %
                            (path, expected_len, phys_len))
          self.logger.error("Url was %s, first 100 bytes:" % url)
          self.logger.error("%s" % contents[0:100])
          self.errors += 1
          check_if_error_threshold_reached(self, self.errors, self.url_timer.num_events)
        contents = contents.decode('utf-8', 'ignore')
        decoded_len = len(contents)
        if decoded_len != phys_len:
          self.logger.warn("%s: Decoded len was %ld, physical len was %d" %
                           (path, decoded_len, phys_len))
        self.bytes_processed += phys_len
        entry = {"path": path,
                 "name": os.path.split(path)[-1],
                 "contents": contents}
        self.pending_entries.append(entry)

  def on_shutdown(self):
    self._commit()
    self.msg_timer.log_final_results(self.logger)
    self.url_timer.log_final_results(self.logger)
    self.log(INFO, "perf: total bytes processed = %d" % self.bytes_processed)
    self.log(INFO, "Total errors: %d" % self.errors)
    
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
