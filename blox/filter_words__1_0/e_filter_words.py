from element import *
import os
import time

class filter_words(Element):
  def on_load(self, config):
    self.name = "Filter-Words"
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["words"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["words"])
    words = [w.rstrip() for w in open(os.path.expanduser('~/Downloads/WORD.LST'))]
    self.word_hash = {}
    for word in words:
      self.word_hash[word] = True

  def recv_push(self, port, log):
    new_log = {}
    log = log.log
    words = log["word"]
    dict_words = []
  
    for word in words:
      subwords = word.split()
      res = True
      for subword in subwords:
        if not self.word_hash.has_key(subword):
          res = False
      if res == True:
        dict_words.append(word)
    
    if dict_words != []:
      new_log["word"] = dict_words
      nl = Log()
      nl.set_log(new_log)
      self.push("output", nl)