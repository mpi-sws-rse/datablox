#!/usr/bin/env python
# This file is a Datablox-specific wrapper on top of the Engage bootrapper and installer.
import os
import os.path
import sys
from optparse import OptionParser
import traceback
import subprocess
import tempfile
import random
import string

import bootstrap

def main(argv):
    usage = "usage: %prog [options] deployment_home"
    parser = OptionParser(usage=usage)
    (options, args) = parser.parse_args(args=argv)
    if len(args) != 1:
        parser.error("Expecting exactly one argument, the directory to install datablox (a.k.a. the deployment home)")
    dh = os.path.abspath(os.path.expanduser(args[0]))
    if os.path.exists(dh):
        parser.error("Installation directory %s already exists - delete it if you want to install to that directory." % dh)

    print "Deploying datablox to %s" % dh
    print "  Running Engage bootstrap"
    bt_log = os.path.join(dh, "log/bootstrap.log")
    try:
        rc = bootstrap.main(["-c", dh])
    except:
        traceback.print_exc()
        print "Error running Engage bootstrap, check logfile %s for details" % \
              bt_log
        return 1
    if rc != 0:
        print "Engage bootstrap failed, return code was %d. Check logfile %s for details." %\
              (rc, bt_log)
        return 1
    print "  Running install of datablox"
    install_exe = os.path.join(dh, "engage/bin/install")
    if not os.path.exists(install_exe):
        raise Exception("Could not find Engage installer script at %s" %
                        install_exe)
    di_log = os.path.join(dh, "log/install.log")
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(''.join([random.choice(string.letters+string.digits) for i in range(10)]))
        fname = f.name
    try:
        rc = subprocess.call([install_exe, "-p", fname, "datablox"])
    except:
        traceback.print_exc()
        print "Error running datablox install, check logfile %s for details" % \
              di_log
        return 1
    finally:
        os.remove(fname)
    if rc != 0:
        print "Engage install of Datablox caretaker failed, return code was %d. Check logfile %s for details." % \
              (rc, di_log)
        return 1
    print "Datablox deployment successful"
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
