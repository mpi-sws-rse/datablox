#!/usr/bin/env python
# -*- py-indent-offset:2 -*-
"""
This is a test for our file server. Given the file server's key and a list of
files, it requests each of the files in a random order, computing average times.
It can be run with more than one worker, using the --num-workers option. Each
worker is run in a separate thread and uses its own random sequence of file
accesses.
"""
import os
import random
import sys
from os.path import abspath, join, dirname, exists, expanduser
from optparse import OptionParser
import urllib
from Crypto.Cipher import DES
from multiprocessing import Process, Queue, JoinableQueue
import time




file_server_keypath = abspath(expanduser("~/apps/datablox_file_server_key"))

FILE_SERVER_KEY=None

def _format_time(tv):
    if round(tv, 0) > 0.0:
        return "%.3f s" % tv
    else:
        tv = tv * 1000.0
        if round(tv, 0) > 0.0:
            return "%.3f ms" % tv
        else:
            return "%f ms" % tv

class PerfCounter(object):
    def __init__(self, block_name, counter_name):
        self.block_name = block_name
        self.counter_name = counter_name
        self.num_events = 0
        self.total_duration = 0.0
        self.timer = None

    def start_timer(self):
        assert self.timer == None
        self.timer = time.time()

    def stop_timer(self, num_events=1):
        assert self.timer != None
        self.total_duration += time.time() - self.timer
        self.num_events += num_events
        self.timer = None

    def abort_timer(self):
      assert self.timer
      self.timer = None

    def _format_stats(self):
        avg = 0.0 if self.num_events==0 else self.total_duration/float(self.num_events)
        return "perf: %s.%s: events: %d, total duration: %s, avg: %s" % \
               (self.block_name, self.counter_name,
                self.num_events, _format_time(self.total_duration),
                _format_time(avg))

    def __repr__(self):
        return self._format_stats()
    
    def log_final_results(self, logger):
        if self.timer != None:
            logger.warn("Timer for performance counter %s.%s not stopped!" %
                        self.block_name, self.counter_name)
        logger.info(self._format_stats())

    @staticmethod
    def combine_counters(counter_list, block_name=None, counter_name=None):
        assert len(counter_list)>0
        if not block_name:
            block_name = counter_list[0].block_name
        if not counter_name:
            counter_name = counter_list[0].counter_name
        sum_counter = PerfCounter(block_name, counter_name)
        for c in counter_list:
            sum_counter.num_events += c.num_events
            sum_counter.total_duration += c.total_duration
        return sum_counter

    @staticmethod
    def average_counters(counter_list, block_name=None, counter_name=None):
      num_counters = len(counter_list)
      assert num_counters>0
      if not block_name:
        block_name = counter_list[0].block_name
      if not counter_name:
        counter_name = counter_list[0].counter_name
      num_events = 0
      total_duration = 0.0
      for c in counter_list:
        num_events += c.num_events
        total_duration += c.total_duration
      sum_counter = PerfCounter(block_name, counter_name)
      sum_counter.num_events = num_events/num_counters
      sum_counter.total_duration = total_duration/float(num_counters)
      return sum_counter


def generate_url_for_path(path, key_file, server_ip):
  global FILE_SERVER_KEY
  path = path.encode('utf-8')
  if FILE_SERVER_KEY==None:
    with open(key_file, 'r') as f:
      FILE_SERVER_KEY = f.read()
  obj = DES.new(FILE_SERVER_KEY, DES.MODE_ECB)
  padding = ''
  for i in range(0 if len(path)%8 == 0 else 8 - (len(path)%8)):
    padding += '/'
  path = padding + path
  enc_path = obj.encrypt(path)
  url_path = urllib.quote(enc_path)
  return "http://" + server_ip + ":4990/?key=" + url_path

def fetch_file(url):
  opener = urllib.FancyURLopener({})
  f = opener.open(url)
  return f.read()

def read_filelist(filelist_file):
  file_list = []
  with open(filelist_file, "r") as f:
    for line in f:
      file_list.append(line.rstrip())
  random.shuffle(file_list)
  return file_list
      
def run_worker(worker_idx, server_ip, q1, q2):
  (key_file, filelist_filename) = q1.get()
  file_list = read_filelist(filelist_filename)
  pc = PerfCounter("fileserver", "reqs")
  with open(key_file, "r") as kf:
    key = kf.read()
  q1.task_done()
  size = 0
  start_token = q2.get()
  errors = 0
  for filename in file_list:
    url = generate_url_for_path(filename, key_file, server_ip)
    pc.start_timer()
    try:
      data = fetch_file(url)
    except:
      errors += 1
      pc.abort_timer()
      data = None
    if data!=None:
      size += len(data)
      pc.stop_timer()
  q1.put((worker_idx, pc, size, errors),)


def _format_avg_size(total_size, num_files, total_time):
  if total_size==0 or num_files==0:
    return "Unable to compute averages: total_size=%d num_files=%d" % \
           (total_size, num_files)
  avg_size_per_file = float(total_size)/float(num_files)/1000000.0
  time_per_file = total_time/float(num_files)
  time_per_megabyte = total_time/(float(total_size)/1000000.0)
  return "%d files, %3f mb/file, %s/file %s/mb" % \
         (num_files, avg_size_per_file, _format_time(time_per_file),
          _format_time(time_per_megabyte))
    
def coordinate_workers(num_workers, key_file, server_ip, file_list_filename):
  q1 = JoinableQueue()
  q2 = Queue()
  workers = []
  print "starting workers"
  for worker_idx in range(num_workers):
    q1.put((key_file, file_list_filename),)
    p = Process(target=run_worker, args=(worker_idx, server_ip, q1, q2))
    workers.append(p)
    p.start()
  print "waiting for workers to finish initialization"
  q1.join()
  print "sending start tokens"
  for worker_idx in range(num_workers): q2.put(worker_idx)
  print "waiting for results"
  results = []
  total_size = 0
  num_files = None
  total_errors = 0
  for i in range(num_workers):
    (worker_num, result, worker_total_size, worker_errors) = q1.get()
    print "Result for worker %d:\n  %s\n  %s\n  %d errors" % \
          (worker_num, result,
           _format_avg_size(worker_total_size, result.num_events,
                            result.total_duration),
           worker_errors)
    total_size += worker_total_size
    total_errors += worker_errors
    if num_files:
      assert result.num_events==num_files, \
             "Worker %d saw %d files, which does not agree with previous count of %d"%\
             (worker_num, result.num_events, num_files)
    else:
      num_files = result.num_events
    results.append(result)
  print "got all results"
  all = PerfCounter.average_counters(results)
  print all
  print _format_avg_size(float(total_size)/float(num_workers), num_files,
                         all.total_duration)
  bw = (float(total_size)/1000000.0)/all.total_duration
  print "Total Bandwidth: %.2f mb/s" % bw
  print "Total errors: %d" % total_errors
  return 0

def generate_file_list(root_dir, file_list_filename):
  with open(file_list_filename, "w") as f:
    for root, dirnames, filenames in os.walk(abspath(root_dir)):
      for filename in filenames:
        f.write(join(root, filename)+"\n")
  return 0


def main(argv):
  usage = "\n%prog [options] server_ip file_list_filename\n%prog --generate-file-list root_dir file_list_filename"
  parser = OptionParser(usage=usage)
  parser.add_option("-w", "--num-workers", dest="num_workers", default=1,
                    type="int", help="Number of workers")
  parser.add_option("-k", "--key-file", dest="key_file",
                    default=file_server_keypath,
                    help="Path to key file (defaults to %s)" % file_server_keypath)
  parser.add_option("-g", "--generate-file-list", dest="generate_file_list",
                    default=False, action="store_true",
                    help="If specified, generate a file list and exit")
  (options, args) = parser.parse_args(argv)
  if len(args)==0:
    parser.print_help()
    return 1
  if options.generate_file_list:
    if len(args)!=2:
      parser.error("Need to specify root_dir and file_list_filename")
    root_dir = args[0]
    file_list_filename = args[1]
    if not os.path.isdir(root_dir):
      parser.error("Root directory %s does not exist" % root_dir)
    return generate_file_list(root_dir, file_list_filename)
  else:
    if len(args)!=2:
        parser.error("Need to specify server_ip and file_list_filename")
    if not os.path.exists(options.key_file):
      parser.error("Key file %s does not exist" % options.key_file)
    server_ip = args[0]
    file_list_filename = args[1]
    if not os.path.exists(file_list_filename):
      parser.error("File list file %s does not exist" % file_list_filename)
    return coordinate_workers(options.num_workers, options.key_file,
                              server_ip, file_list_filename)

if __name__ == "__main__":
  sys.exit(main(sys.argv[1:]))
