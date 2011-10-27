from element import *

class FileQuery(Element):
  name = "File-query"
  
  def on_load(self, config):
    self.name = "File-query"
    self.add_port("meta_query", Port.PULL, Port.UNNAMED, ["name"])
    self.add_port("data_query", Port.PULL, Port.UNNAMED, ["query"])
    self.interactive = False
    self.query = "lucene"
    self.category = "Source"
    print "File-query element loaded"

  def src_start(self):
    self.start_query()
    
  def start_query(self):
    log = Log()
    log.log["query"] = self.query
    self.pull("data_query", log)
  
  def do_interactive(self):
    print
    print "Hit enter with no input to quit."
    query = raw_input("Query:")
    if query == '':
        self.interactive = False
        self.shutdown()
    print
    category = raw_input("Category:")
    category = category.capitalize()
    print "Searching for: " + query + " in category " + category
    self.query = query
    self.category = category
    self.start_query()
    
  def recv_pull_result(self, port_name, log):
    if port_name == self.full_port_name("data_query"):
      # print self.name + " got document results " + str(log.log)
      ml = Log()
      ml.log["name"] = log.log["results"]
      self.pull("meta_query", ml)
    elif port_name == self.full_port_name("meta_query"):
      log = log.log
      # print self.name + " got metadata for the documents " + str(log)
      print "Showing results under category " + self.category
      paths = log["path"]
      categories = log["category"]
      for i in range(0, len(paths)):
        if categories[i] == self.category:
          print paths[i]
      if self.interactive:
        self.do_interactive()
      else:
        self.shutdown()