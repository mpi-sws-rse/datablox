from block import *
import time
import itertools
from logging import ERROR, WARN, INFO, DEBUG

class permute(Block):
  def get_words(self):
    return self.config["words"]
    
  def do_task(self):
    sleeptime = self.config["sleep"] if self.config.has_key("sleep") else 0
    for word in self.get_words():
      t = time.time()
      #add a space to split words
      word += ' '
      work_queue = []
      word_hash = {}
      for p in itertools.permutations(word):
        pw = ''.join(p).strip()
        if word_hash.has_key(pw):
          continue
        else:
          word_hash[pw] = True          
          work_queue.append(pw)
          if len(work_queue) > self.log_len:
            log = Log()
            log.log["word"] = []
            #TODO: just copy the list instead
            log.log["word"].extend(work_queue)
            self.push("output", log)
            work_queue = []
            if time.time() - t > 3:
              yield
              t = time.time()
      if work_queue != []:
        log = Log()
        log.log["word"] = []
        #TODO: just copy the list instead
        log.log["word"].extend(work_queue)
        self.push("output", log)

  def on_load(self, config):
    self.config = config
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["words"])
    self.log_len = 1000
    self.log(INFO, "Permute block loaded")