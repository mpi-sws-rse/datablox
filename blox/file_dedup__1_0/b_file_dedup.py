import redis
from block import *
from logging import ERROR, WARN, INFO, DEBUG

class file_dedup(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["url", "fingerprint"])
    self.add_port("store", Port.QUERY, Port.UNNAMED, ["chunk"])
    self.add_port("store_control", Port.QUERY, Port.UNNAMED, ["command", "args"])
    self.add_port("control", Port.QUERY, Port.UNNAMED, ["command", "args"])
    self.chunk_index = redis.StrictRedis()
    self.total_chunks = 0
    self.duplicate_chunks = 0

  def add_file(self, url):
    log = Log()
    log.append_field("chunk", [url])
    chunk_id_log = self.query("store", log)
    assert(len(chunk_id_log.log["result"]) == 1)
    return chunk_id_log.log["result"][0]

  def recv_push(self, port, log):
    for url, fp in log.iter_fields("url", "fingerprint"):
      res = self.chunk_index.get(fp)
      if res:
        chunk_id, refs = json.loads(res)
        refs += 1
        self.chunk_index.set(fp, json.dumps([chunk_id, refs]))
        self.duplicate_chunks += 1
      else:
        chunk_id = self.add_file(url)
        self.chunk_index.set(fp, json.dumps([chunk_id, 1]))
        self.total_chunks += 1
  
  def on_shutdown(self):
    self.log(INFO, "%s: total chunks added: %d, duplicate chunks: %d" % (self.id, self.total_chunks, self.duplicate_chunks))
    
  def return_store_urls(self, port, fps):
    #print [self.chunk_index.get(fp) for fp in fps]
    try:
      chunk_ids = [json.loads(self.chunk_index.get(fp))[0] for fp in fps]
      #self.log(INFO, chunk_ids)
      log = Log()
      log.append_field("command", ["restore"])
      log.append_field("args", [chunk_ids])
      res_log = self.query("store_control", log)
    except Exception as e:
      self.log(WARN, "could not restore urls")
      res_log = Log()
      res_log.append_field("chunk", [])

    self.return_query_res(port, res_log)
  
  def delete(self, fp):
    res = self.chunk_index.get(fp)
    if res:
      chunk_id, refs = json.loads(res)
      if refs == 1:
        slog = Log()
        slog.append_field("command", ["delete"])
        slog.append_field("args", [[chunk_id]])
        deleteres = self.query("store_control", slog).log["result"]
        assert(len(deleteres)==1)
        res = deleteres[0] and self.chunk_index.delete(fp)
        self.total_chunks -= 1
        self.log(INFO, "deleted fp %r (file %r)" % (fp, chunk_id))
        return res
      else:
        refs -= 1
        self.chunk_index.set(fp, json.dumps([chunk_id, refs]))
        self.duplicate_chunks -= 1
        self.log(INFO, "decremented ref count for %r (file %r) to %r" % (fp, chunk_id, refs))
        return True
    else:
      self.log(WARN, "No chunk with ID %r to delete" % fp)
      return False
    
  def recv_query(self, port, log):
    assert(len(log.log["command"]) == 1)
    assert(len(log.log["args"]) == 1)
    command, args = log.log["command"][0], log.log["args"][0]
    if command == "delete":
      results = [self.delete(url) for url in args]        
      ret = Log()
      ret.append_field("result", results)
      self.return_query_res(port, ret)
    elif command == "restore":
      self.return_store_urls(port, args)