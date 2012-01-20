from block import *

class backup_manager(Block):
  def on_load(self, config):
    self.name = "Backup-Manager"
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["path", "chunks", "fingerprints"])
    self.add_port("chunk_index", Port.PUSH, Port.UNNAMED, ["chunk", "fingerprint"])
    self.add_port("meta_index", Port.PUSH, Port.UNNAMED, ["path", "fingerprints"])
  
  def add_meta(self, log):
    commands = ["ADD" for p in log.log["path"]]
    args = [(p, f) for p, f in zip(log.log["path"], log.log["fingerprints"])]
    mlog = Log()
    mlog.append_field("command", commands)
    mlog.append_field("args", args)
    self.push("meta_index", mlog)

  def add_chunks(self, log):
    chunks = []
    fps = []
    for path, clist, fplist in log.iter_fields("path", "chunks", "fingerprints"):
      chunks.extend(clist)
      fps.extend(fplist)

    clog = Log()
    clog.append_field("chunk", chunks)
    clog.append_field("fingerprint", fps)
    self.buffered_push("chunk_index", clog)
        
  def recv_push(self, port, log):
    self.add_chunks(log)
    self.add_meta(log)