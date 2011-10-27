import os
import sys
from element import *

all_element_classes = []
elements = []

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
  
  inst = element()
  inst.on_load(config)
  elements.append(inst)
  return inst

def setup_connections():
  # crawler1 = create_element("Dir-Src", {"directory": "."})
  crawler2 = create_element("Dir-Src", {"directory": "/Users/saideep/Downloads/pylucene-3.4.0-1/samples"})
  categorizer = create_element("Categorize", {})
  indexer = create_element("Lucene-index", {"crawlers": 1})
  metaindexer = create_element("File-mongo", {"crawlers": 1})
  query = create_element("File-query", {})
  
  # crawler1.connect("output", categorizer, "input")
  crawler2.connect("output", categorizer, "input")
  categorizer.connect("output", indexer, "input")
  categorizer.connect("output", metaindexer, "input")
  query.connect("meta_query", metaindexer, "file_data")
  query.connect("data_query", indexer, "query")
  
def start_elements():
  for e in elements:
    print "starting " + e.name
    e.start()
  
if __name__ == "__main__":
  load_elements(os.environ["BLOXPATH"])
  setup_connections()
  start_elements()