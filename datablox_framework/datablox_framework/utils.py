
# -*- py-indent-offset:2 -*-

import os
import sys
import subprocess
import re

defunct_re = re.compile("<defunct>$")

def is_process_alive(pid):
  if sys.platform=="linux2" and (not os.path.exists("/proc/%d" % pid)):
    return False
  subproc = subprocess.Popen(["ps", "-p", pid.__str__()],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
  subproc.stdin.close()
  result = subproc.stdout.read().splitlines() # first line is always header
  if len(result)==1:
    return False
  data = result[1]
  # if on linux, we treat defucnt processes as dead
  if sys.platform=="linux2" and defunct_re.search(data):
    return False
  else:
    return True
