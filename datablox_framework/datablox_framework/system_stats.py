"""Package up various system statistics obtained from psutil package.
The caretaker creates a SystemStatsTaker object and periodically calls
take_snapshot(), which gathers the statistics
(which should be cumulative) and puts them in JSON-friendly form
to be sent back to the master.

"""
import sys
import socket
import logging
logger = logging.getLogger(__name__)

from engage_utils import text_tables

try:
    HOSTNAME = socket.gethostname()
    if '.' in HOSTNAME: # remove the domain name
        HOSTNAME = HOSTNAME[:HOSTNAME.index('.')]
except:
    HOSTNAME = 'localhost'

def _cpu_times_to_alist(t):
    l = [('user', t.user), ('nice', t.nice),
         ('system', t.system), ('idle', t.idle)]
    def append_attr(n):
        if hasattr(t, n):
            l.append((n, getattr(t, n),))
    append_attr('iowait')
    append_attr('irq')
    append_attr('softirq')
    return l
                     

class SystemStatsTaker(object):
    def __init__(self):
        try:
            import psutil
            self.psutil = psutil
        except ImportError, e:
            logger.error("Unable to import psutil package, is it installed? Will skip gathering of system statistics.")
            logger.exception(e)
            self.psutil = None

    def stats_available(self):
        return self.psutil != None

    def take_snapshot(self):
        """Return the snapshot in JSON-friendly form, or None if psutil
        is not available.
        """
        if self.psutil==None: return None
        vmstat = self.psutil.virtual_memory()
        swap = self.psutil.swap_memory()
        stats = {
            'host':HOSTNAME,
            'all_cpu':_cpu_times_to_alist(self.psutil.cpu_times(percpu=False)),
            'per_cpu':[_cpu_times_to_alist(t) for t in self.psutil.cpu_times(percpu=True)],
            'total_mem':vmstat.total,
            'avail_mem':vmstat.available,
            'swap_in':swap.sin,
            'swap_out':swap.sout
        }
        return stats

def _diff(l1, l2):
    return [(v2[0], v2[1] - v1[1]) for (v1, v2) in zip(l1, l2)]


def _compute_cpu_fractions(cpu_snapshot1, cpu_snapshot2):
    d = _diff(cpu_snapshot1, cpu_snapshot2)
    total = float(sum([v for (n, v) in d]))
    return [(n, float(v)/total) for (n, v) in d]

MB = 1024.0*1024.0
GB = MB*1024.0

class SystemStats(object):
    """Track the stats for a host on the master.
    """
    def __init__(self, hostname):
        self.hostname = hostname
        self.last_snapshot = None
        self.total_mem = None
        self.available_mem = None
        self.swap_in = None
        self.swap_out = None
        self.all_cpu_stats = None
        self.per_cpu_stats = None

    def add_snapshot(self, stats_snapshot):
        self.total_mem = stats_snapshot['total_mem']
        self.available_mem = stats_snapshot['avail_mem']
        if self.last_snapshot==None:
            self.last_snapshot = stats_snapshot
            return # nothing else to do until we get a second snapshot
        self.swap_in = stats_snapshot['swap_in'] - self.last_snapshot['swap_in']
        self.swap_out = stats_snapshot['swap_out'] - self.last_snapshot['swap_out']
        self.all_cpu_stats = _compute_cpu_fractions(self.last_snapshot['all_cpu'],
                                                    stats_snapshot['all_cpu'])
        self.per_cpu_stats = [_compute_cpu_fractions(last, current) for (last, current)
                              in zip(self.last_snapshot['per_cpu'],
                                     stats_snapshot['per_cpu'])]
        self.last_snapshot = stats_snapshot

    @staticmethod
    def create_cpu_stats_table():
        """Return a text table definition for cpu stats
        """
        return text_tables.Table(
            [text_tables.LeftAlignedCol('host', 15),
            text_tables.RightAlignedCol('cpu', 3)] +
            [text_tables.PctCol(hdr, 2, include_percentage_symbol=False) for
             hdr in ['%user', '%nice', '%sys',
                     '%idle', '%iowait', '%irq',
                     '%softirq']])

    def add_rows_to_cpu_stats_table(self, t):
        if self.all_cpu_stats==None:
            t.add_row([self.hostname, 'all', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A'])
            return
        
        def get_values(cpu_name, stats):
            r = [self.hostname, cpu_name] + [v for (n, v) in stats]
            while len(r)<9:
                r.append('N/A')
            return r
        if len(self.per_cpu_stats)>1:
            t.add_row(get_values('all', self.all_cpu_stats))
        for i in range(len(self.per_cpu_stats)):
            t.add_row(get_values(str(i), self.per_cpu_stats[i]))

    @staticmethod
    def create_memory_stats_table():
        return text_tables.Table([
            text_tables.LeftAlignedCol('host', 15),
            text_tables.FloatCol('Total Mem (GB)', 3, 2),
            text_tables.FloatCol('Avail Mem (GB)', 3, 2),
            text_tables.PctCol('Avail Mem', 1),
            text_tables.FloatCol('Swap In (MB)', 4, 3),
            text_tables.FloatCol('Swap Out (MB)', 4, 3)
            ])

    def add_row_to_memory_stats_table(self, t):
        t.add_row([self.hostname,
                   float(self.total_mem)/GB if self.total_mem else 'N/A',
                   float(self.available_mem)/GB if self.available_mem!=None else 'N/A',
                   float(self.available_mem)/float(self.total_mem) \
                     if self.available_mem!=None and self.total_mem and self.total_mem!=0 \
                     else 'N/A',
                   float(self.swap_in)/MB if self.swap_in!=None else 'N/A',
                   float(self.swap_out)/MB if self.swap_out!=None else 'N/A'])

def test(interval=30):
    import time
    taker = SystemStatsTaker()
    tracker = SystemStats(HOSTNAME)
    tracker.add_snapshot(taker.take_snapshot())
    ctbl = SystemStats.create_cpu_stats_table()
    mtbl = SystemStats.create_memory_stats_table()
    print "Waiting %d seconds for first interval" % interval
    while True:
        time.sleep(interval)
        tracker.add_snapshot(taker.take_snapshot())
        tracker.add_rows_to_cpu_stats_table(ctbl)
        ctbl.write_to_stream(sys.stdout)
        ctbl.clear_rows()
        print
        tracker.add_row_to_memory_stats_table(mtbl)
        mtbl.write_to_stream(sys.stdout)
        mtbl.clear_rows()
        print

if __name__ == "__main__":
    if len(sys.argv)>=2:
        test(int(sys.argv[1]))
    else:
        test()
