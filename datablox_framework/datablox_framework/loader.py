import os
import sys
from element import *

all_element_classes = []
elements = []
#this is not thread safe, so call this before starting threads
port_num_gen = PortNumberGenerator()

def load_elements(path):
  global all_elements    

  for name in os.listdir(path):
    if name.endswith(".py") and name.startswith("e_"):
      modulename = os.path.splitext(name)[0]
      print "importing: " + modulename
      __import__(modulename)
  
  element_classes = Element.__subclasses__()

  for element_class in element_classes:
    all_element_classes.append(element_class)
    
def create_element(name, config):
  element = None
  for e in all_element_classes:
    if e.name == name:
      element = e

  if element == None:
    print "Could not find element with name " + name
    raise NameError
  
  inst = element(port_num_gen)
  inst.on_load(config)
  elements.append(inst)
  return inst

def setup_connections():
  crawler1 = create_element("Dir-Src", {"directory": "."})
  crawler2 = create_element("Dir-Src", {"directory": "/Users/saideep/Downloads/pylucene-3.4.0-1/samples"})
  join = create_element("Join", {"joins": 2})
  categorizer = create_element("Categorize", {})
  indexer = create_element("Solr-index", {"crawlers": 2})
  metaindexer = create_element("File-mongo", {"crawlers": 2})
  query = create_element("File-query", {})
  
  crawler1.connect("output", join, "input1")
  crawler2.connect("output", join, "input2")
  join.connect("output", categorizer, "input")
  categorizer.connect("output", indexer, "input")
  categorizer.connect("output", metaindexer, "input")
  query.connect("meta_query", metaindexer, "file_data")
  query.connect("data_query", indexer, "query")
  
  # source1 = create_element("Dir-Src", {"directory": "."})
  # source2 = create_element("Dir-Src", {"directory": "/Users/saideep/Downloads/pylucene-3.4.0-1/samples"})
  # source3 = create_element("Dir-Src", {"directory": "/Users/saideep/Projects/sandbox"})
  # trans1 = create_element("Categorize", {})
  # trans2 = create_element("Categorize", {})
  # join1 = create_element("Join", {"joins": 2})
  # join2 = create_element("Join", {"joins": 2})
  # sink = create_element("Solr-index", {"crawlers": 3})
  # #sink = create_element("Null", {})
  # 
  # source1.connect("output", trans1, "input")
  # source2.connect("output", join1, "input1")
  # source3.connect("output", join1, "input2")
  # trans1.connect("output", join2, "input1")
  # join1.connect("output", join2, "input2")
  # join2.connect("output", sink, "input")

  # source = create_element("Simple-pull-client", {})
  # sink = create_element("Simple-pull", {})
  # source.connect("output", sink, "input")
  
def start_elements():
  for e in elements:
    print "starting " + e.name
    e.start()
  
if __name__ == "__main__":
  load_elements(os.environ["BLOXPATH"])
  setup_connections()
  start_elements()