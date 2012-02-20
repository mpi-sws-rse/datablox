from block import *
import os
import time
from logging import ERROR, WARN, INFO, DEBUG

class meta_store(Block):
  def on_load(self, config):
    import pymongo
    from pymongo import Connection
    
    self.config = config
    self.connection = Connection()
    self.added = []
    self.removed = []
    db = self.connection['metadb']
    self.file_index = db.file_index
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "time", "assets"])
    self.add_port("control", Port.QUERY, Port.UNNAMED, ["command", "args"])
  
  # def update_refs(self, path, fps):
  #   old_fps = list(self.file_index.find({"path": path}, {"time": time}, {"related": 1}))
  #   assert(len(old_fps) <= 1)
  #   self.file_index.remove({"path" : path})
  #   if len(old_fps) > 0:
  #     old = set(old_fps[0]["fingerprints"])
  #     new = set(fps)
  #     self.removed.append(old - new)
  #     self.added.append(new - old)
  
  def recv_push(self, port, log):
    entries = [{"path": path, "time": time, "assets": assets} for path, time, assets in log.iter_fields("path", "time", "assets")]
    self.file_index.insert(entries)

  def delete(self, path, time):
    try:
      self.file_index.remove({"path" : path, "time": time})
      return True
    except Exception as e:
      self.log(WARN, "Got exception on delete: %r" % e)
      return False
  
  def restore(self, path, time):
    res = []
    asset_objs = list(self.file_index.find({"path": path, "time": time}, fields=["assets"]))
    assert(len(asset_objs)<=1)
    if asset_objs == []:
      self.log(WARN, "could not get any assets for url: %r time: %r" % (path, time))
      assets = []
    else:
      assets = asset_objs[0]["assets"]
      return assets
    
  def recv_query(self, port, log):
    assert(len(log.log["command"]) == 1)
    assert(len(log.log["args"]) == 1)
    command, args = log.log["command"][0], log.log["args"][0]
    retlog = Log()
    
    if command == "list":
      recs = list(self.file_index.find({}, fields=["path", "time"]))
      paths = [r["path"] for r in recs]
      times = [r["time"] for r in recs]
      retlog.append_field("path", paths)
      retlog.append_field("time", times)
    elif command == "restore":
      self.log(INFO, "args: %r" % args)
      asset_lists = [self.restore(path, time) for path, time in args]
      retlog.append_field("assets", asset_lists)
    elif command == "delete":
      results = [self.delete(path, time) for path, time in args]
      retlog.append_field("result", results)

    self.return_query_res(port, retlog)

  def on_shutdown(self):
    self.connection.disconnect()
