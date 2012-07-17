import xmlrpclib
import json
import cgi
import time
import sys
import random

if len(sys.argv) != 4:
  print "%s: requires a URL listing file name and two indices as arguments"
  sys.exit(1)

proxy = xmlrpclib.ServerProxy("http://localhost:8000/")
with open(sys.argv[1]) as f:
  urls = f.readlines()

start_index = int(sys.argv[2])
end_index = int(sys.argv[3])
urls = urls[start_index: end_index]
urls = [u.strip() for u in urls]

list_times = []
fetch_times = []
sleep_time = 1

print "Workload %d urls" % (len(urls))

print "start time: %r" % (time.ctime())

for url in urls:
  print "adding url: %r, time: %r" % (url, time.ctime())
  log = {"internet_url": [url]}
  res = json.loads(proxy.bookmark(json.dumps(log)))["result"]
  assert(res == True)
  time.sleep(sleep_time)
  start = time.time()
  bookmarks = json.loads(proxy.list(json.dumps({})))
  duration = time.time() - start
  print "listing bookmarks took: %r" % duration
  list_times.append(duration)
  time.sleep(sleep_time)
  if bookmarks["url"] != []:
    i = random.randrange(0, len(bookmarks["url"]))
    log = {"url": [bookmarks["url"][i]], "time": [bookmarks["time"][i]]}
    start = time.time()
    link = json.loads(proxy.restore(json.dumps(log)))["url"][0]
    duration = time.time() - start
    print "fetching bookmark took: %r" % duration
    fetch_times.append(duration)
  else:
    print "No bookmarks yet to fetch"

print "Done"
print "Average list time:", sum(list_times)/len(list_times)
print "Average fetch time:", sum(fetch_times)/len(fetch_times)
# proxy.shutdown()
sys.exit(0)