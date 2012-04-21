from block import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG


import filetype_utils

def p2f(p):
  """Convert a path (including volume name) into a filename)
  """
  i = p.find(":")
  assert i!=(-1), \
         "Unexpected path format: %s, expecting volume name" % p
  return p[i+1:]

class stats(object):
  def __init__(self):
    self.incoming_msgs = 0
    self.incoming_records = 0
    self.filtered_msgs = 0
    self.filtered_records = 0

  def add_msg(self, records, filtered_records):
    self.incoming_msgs += 1
    self.incoming_records += records
    if filtered_records>0:
      self.filtered_msgs += 1
      self.filtered_records += filtered_records


class filename_categorizer(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "size", "perm", "owner"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["path", "name", "size", "perm", "owner", "filetype", "category"])
    self.add_port("indexable_output", Port.PUSH, Port.UNNAMED, ["path", "name", "size", "perm", "owner", "url"])
    self.stats = stats()

  def indexable_file(self, path):
    """If true, the file can be indexed by the indexing engine (e.g. contains
    text content). We will only send the data if this returns True.
    """
    return filetype_utils.is_indexable_file(path)

  def get_categories(self, log):
    paths = log["path"]
    filetypes = []
    categories = []
    tag_lists = []
    for p in paths:
      filepath = p2f(p)
      (filetype,category) = \
          filetype_utils.get_file_description_and_category(filepath)
      filetypes.append(filetype)
      categories.append(category)
      tags = filetype_utils.get_tags(filepath)
      tag_lists.append(tags)
    return (filetypes, categories, tag_lists)
  
  def recv_push(self, port, log):
    if log.log.has_key("token"):
      self.log(INFO, "got the finish token for directory " + log.log["token"][0])
    else:
      (filetypes, categories, tag_lists) = self.get_categories(log.log)
      log.append_field("filetype", filetypes)
      log.append_field("category", categories)
      log.append_field("tags", tag_lists)
          
    self.buffered_push("output", log)
    def include(record):
      if record.has_key("token"):
        return True
      else:
        return filetype_utils.is_indexable_file(p2f(record["path"]))
    filtered_log = log.filtered_log(include)
    filtered_rows = filtered_log.num_rows()
    if filtered_rows>0:
      self.buffered_push("indexable_output", filtered_log)
    self.stats.add_msg(log.num_rows(), filtered_rows)

  def on_shutdown(self):
    self.log(INFO, "Message statistics: ")
    self.log(INFO, "  Incoming: %d messages, %d rows" % (self.stats.incoming_msgs,
                                                         self.stats.incoming_records))
    self.log(INFO, "  Outgoing: %d messages, %d rows" % (self.stats.incoming_msgs,
                                                         self.stats.incoming_records))
    self.log(INFO, "  Filtered: %d messages, %d rows" % (self.stats.filtered_msgs,
                                                         self.stats.filtered_records))



