from element import *

"""
This class is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""

class Ticker(object):
  def __init__(self):
    self.tick = True

  def run(self):
    while self.tick:
      sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(1.0)

class IndexFiles(object):
  def __init__(self, storeDir):
    if not os.path.exists(storeDir):
      os.mkdir(storeDir)
    self.store = lucene.SimpleFSDirectory(lucene.File(storeDir))
    self.analyzer = lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
    # clear the index initially
    self.writer = lucene.IndexWriter(self.store, self.analyzer, True,
                                lucene.IndexWriter.MaxFieldLength.LIMITED)
    self.writer.setMaxFieldLength(1048576)
  
  def optimize_index(self):
    self.writer.optimize()
    
  def teardown(self):
    # print 'optimizing index',
    # threading.Thread(target=ticker.run).start()
    self.optimize_index()
    self.writer.close()
    # ticker.tick = False
    # print 'done'
  
  def can_be_indexed(self, path):
    return (path.endswith('.py') or path.endswith('.txt'))
    
  def index_docs(self, files):
    for path in files:
      if not self.can_be_indexed(path):
        continue
      # print "adding", path
      try:
        file = open(path)
        contents = unicode(file.read(), 'iso-8859-1')
        file.close()
        doc = lucene.Document()
        doc.add(lucene.Field("name", path,
                             lucene.Field.Store.YES,
                             lucene.Field.Index.NOT_ANALYZED))
        doc.add(lucene.Field("path", path,
                             lucene.Field.Store.YES,
                             lucene.Field.Index.NOT_ANALYZED))
        if len(contents) > 0:
            doc.add(lucene.Field("contents", contents,
                                 lucene.Field.Store.NO,
                                 lucene.Field.Index.ANALYZED))
        else:
            print "warning: no content in %s" % path
        self.writer.addDocument(doc)
      except Exception, e:
          print "Failed in indexDocs:", e
  
  def search_index(self, command):
    self.teardown()
    self.writer = lucene.IndexWriter(self.store, self.analyzer, False,
                                lucene.IndexWriter.MaxFieldLength.LIMITED)
    self.writer.setMaxFieldLength(1048576)
    self.searcher = IndexSearcher(self.store, True)
    
    query = QueryParser(Version.LUCENE_CURRENT, "contents",
                        self.analyzer).parse(command)
    scoreDocs = self.searcher.search(query, 50).scoreDocs
    print "%s total matching documents." % len(scoreDocs)
    paths = []

    for scoreDoc in scoreDocs:
      doc = self.searcher.doc(scoreDoc.doc)
      paths.append(doc.get("path"))
      # print 'path:', doc.get("path"), 'name:', doc.get("name")
      
    return paths

class LuceneIndex(Element):
  name = "Lucene-index"
  
  def on_load(self, config):
    import time
    import sys, os, lucene, threading, time
    from datetime import datetime
    from lucene import \
        QueryParser, IndexSearcher, StandardAnalyzer, SimpleFSDirectory, File, \
        VERSION, initVM, Version
    
    self.name = "Lucene-index"
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["name"])
    self.add_port("query", Port.PULL, Port.UNNAMED, ["query"])
    self.vm = lucene.initVM()
    storeDir = "index"
    self.indexer = IndexFiles(storeDir)
    self.crawler_done = False
    self.queries = []
    self.num_tokens = config["crawlers"]
    print "Lucene-index element loaded"
  
  def recv_push(self, port, log):
    self.vm.attachCurrentThread()
    if log.log.has_key("token"):
      print self.name + " got the finish token for the directory " + log.log["token"]
      # self.indexer.optimize_index()
      # self.crawler_done = True
      # self.process_outstanding_queries()
      self.num_tokens = self.num_tokens - 1
      if self.num_tokens == 0:
        self.crawler_done = True
        self.process_outstanding_queries()
    else:
      files = log.log["name"]
      try:
          self.indexer.index_docs(files)
      except Exception, e:
          print "Failed: ", e
  
  def on_shutdown(self):
    self.indexer.teardown()
    
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
    self.vm.attachCurrentThread()
    query = log.log["query"]
    paths = self.indexer.search_index(query)
    nl = Log()
    nl.log["results"] = paths
    self.return_pull(port_name, nl)