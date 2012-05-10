from optparse import OptionParser
import sys
from os.path import expanduser, abspath

import sunburnt
import dist_job_mgr.client as djm

QUERY_TERM="datablox"

def run_query(addresses):
    master = addresses[0]
    si = sunburnt.SolrInterface("http://%s:8983/solr" % master)
    resp = si.query(QUERY_TERM).execute()
    assert resp.status==0
    objs = resp.result.numFound
    time_ms = resp.QTime
    if time_ms>0:
        rate = "%.2f obj/sec" % (1000.0*(float(objs)/float(time_ms)))
    else:
        rate = "Rate too fast to measure"
    print "%d results in %d ms (%s)" % (objs, time_ms, rate)
    return 0

def main(argv=sys.argv[1:]):
    usage = "%prog [options] query_host [host2 host3 ...]"
    parser = OptionParser(usage=usage)
    (options, args) = parser.parse_args(argv)
    if len(args)==0:
        parser.error("Need to provide at least one host name")
    djm_conn = djm.get_local_connection(abspath(expanduser("~/apps/djm")))
    addresses = []
    for name in args:
        host = djm_conn.find_node_by_name(name)
        if host==None:
            parser.error("No node named '%s' found in djm database" % name)
        addresses.append(host["contact_address"])
    return run_query(addresses)


if __name__ == "__main__":
    sys.exit(main())
