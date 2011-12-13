import redis
from element import *

class flat_chunk_index(Element):
  def on_load(self, config):
    self.name = "Flat-Chunk-Index"
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["chunk", "fingerprint"])
    self.add_port("store", Port.PULL, Port.UNNAMED, ["command", "chunk"])
    self.add_port("store_restore", Port.PULL, Port.UNNAMED, ["chunk_id"])
    self.add_port("reference", Port.PULL, Port.UNNAMED, ["command"])
    self.add_port("restore", Port.PULL, Port.UNNAMED, ["fingerprint"])
    self.chunk_index = redis.StrictRedis()
    self.total_chunks = 0
    self.duplicate_chunks = 0

  def add_chunk(self, chunk):
    log = Log()
    log.append_field("chunk", [chunk])
    chunk_id_log = self.pull("store", log)
    assert(len(chunk_id_log.log["result"]) == 1)
    return chunk_id_log.log["result"][0]

  def recv_push(self, port, log):
    for c, fp in log.iter_fields("chunk", "fingerprint"):
      res = self.chunk_index.get(fp)
      if res:
        # print "Chunk already in the database"
        self.duplicate_chunks += 1
      else:
        chunk_id = self.add_chunk(c)
        self.chunk_index.set(fp, chunk_id)
        # print "Chunk wasn't in the database - added to %s" % chunk_id
        self.total_chunks += 1
  
  def on_shutdown(self):
    print "%s: total chunks added: %d, duplicate chunks: %d" % (self.name, self.total_chunks, self.duplicate_chunks)
    
  def return_store_locs(self, port, log):
    fps = log.log["fingerprint"]
    chunk_ids = [self.chunk_index.get(fp) for fp in fps]
    print chunk_ids
    # chunk_ids = []
    # for fp in fps:
    #   i = self.chunk_index.get(fp)
    #   if i == None:
    #     print "**%s Did not get any chunk for hash %s" % (self.name, fp)
    #   chunk_ids.append(i)
    log = Log()
    log.append_field("chunk_id", chunk_ids)
    res_log = self.pull("store_restore", log)
    self.return_pull(port, res_log)
    
  def recv_pull_query(self, port, log):
    if port == "restore":
      self.return_store_locs(port, log)