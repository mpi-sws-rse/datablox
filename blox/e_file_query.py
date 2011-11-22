from element import *

class file_query(Element):
  def on_load(self, config):
    self.name = "File-query"
    self.add_port("meta_query", Port.PULL, Port.UNNAMED, ["name"])
    self.add_port("data_query", Port.PULL, Port.UNNAMED, ["query"])
    self.interactive = True
    print "File-query element loaded"

  def do_task(self):
    if self.interactive:
      self.do_interactive()
    else:
      self.query("lucene", "Source")
      self.shutdown()
    
  def query(self, query, category):
    log = Log()
    log.log["query"] = query
    res = self.pull("data_query", log)
    paths = res.log["results"]
    ml = Log()
    ml.log["paths"] = paths
    res = self.pull("meta_query", ml)
    names = res.log["name"]
    categories = res.log["category"]
    print "Results: "
    for i in range(0, len(names)):
      if categories[i] == category:
        print names[i]
  
  def do_interactive(self):
    #do a sample query, so we know the databases are ready
    self.query("lucene", "Source")
    while self.interactive:
      print
      print "Hit enter with no input to quit."
      query = raw_input("Query:")
      if query == '':
          self.interactive = False
          break
      print
      category = raw_input("Category:")
      category = category.capitalize()
      print "Searching for: " + query + " in category " + category
      self.query(query, category)
      yield