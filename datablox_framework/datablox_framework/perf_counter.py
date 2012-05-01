"""Performance counter utility
"""
import time

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
    """The performance counter is used to track some performance statistic related
    to a given block (e.g. processing time for incoming messages, database access
    time). Initialize the counter in the block's on_load() method and start the
    timer for each event. At the end of the event, stop the timer. If a timer
    call processes multiple logical events (e.g. a message batch), you can pass
    the number into stop_timer(). When the block is shutting down, call
    log_final_results() to dump the performance stats to the logfile.
    """
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
        

import unittest
import logging
logger = logging.getLogger(__name__)

class TestCounters(unittest.TestCase):
    def _add_counter(self, name):
        self.c = PerfCounter("TestCounters", name)

    def testShortCounter(self):
        self._add_counter("short")
        for i in range(1000):
            self.c.start_timer()
            for j in range(i):
                pow(i, j)
            self.c.stop_timer(num_events=(i+1))
        self.assertEqual(self.c.num_events, 500500)
        self.assertTrue(self.c.total_duration > 0.0)
        self.c.log_final_results(logger)

    def testLongCounter(self):
        self._add_counter("long")
        for i in range(5):
            self.c.start_timer()
            time.sleep(1)
            self.c.stop_timer()
        self.assertEqual(self.c.num_events, 5)
        self.assertTrue(self.c.total_duration > 0.0)
        self.c.log_final_results(logger)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

            
                                   
        
    
