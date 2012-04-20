from block import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG


import filetype_utils

class filename_categorizer(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "name", "size", "perm", "owner", "filetype", "category"])
    #self.add_port("indexable_output", Port.PUSH, Port.UNNAMED, ["path", "name", "size", "perm", "owner", "url"])

  def indexable_file(self, path):
    """If true, the file can be indexed by the indexing engine (e.g. contains
    text content). We will only send the data if this returns True.
    """
    return filetype_utils.is_indexable_file(path)

  def get_categories(self, log):
    paths = log["path"]
    filetypes = []
    categories = []
    for p in paths:
      assert p.find(":")!=(-1), \
             "Unexpected path format: %s, expecting volume name" % p
      filename = p[p.find(":")+1:]
      (filetype,category) = \
          filetype_utils.get_file_description_and_category(filename)
      filetypes.append(filetype)
      categories.append(category)
    return (filetypes, categories)
  
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, "got the finish token for directory " + log.log["token"])
    else:
      (filetypes, categories) = self.get_categories(log.log)
      log.append_field("filetype", filetypes)
      log.append_field("category", categories)
          
    self.buffered_push("output", log)
    #self.buffered_push("indexable_output", log)


