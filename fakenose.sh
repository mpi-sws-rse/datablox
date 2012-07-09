#!/bin/bash -ex

# Write fake xunit test results to OUTFILE (defaults to
# "nosetests.xml") based on result of running COMMAND (defaults to
# "make test")

FAILURES="0"
OUTFILE=${OUTFILE:-"nosetests.xml"}
COMMAND=${COMMAND:-"make test"}

if ! $COMMAND ; then
    FAILURES="1"
fi

cat <<EOF > $OUTFILE
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="nosetests" tests="1" errors="0" failures="$FAILURES" skip="0">
  <testcase classname="test_datablox_fake" name="test_datablox_fake" time="100"/>
</testsuite>
EOF
