#!/bin/bash

deployment_home=$1
query=$2
expected_count=$3

count=`$deployment_home/mongodb-2.0/bin/mongo --eval "$2" | grep -E '^[0-9]'`
rc=$?
if [[ "$rc" != "0" ]]; then
  echo "Query $query failed"
  exit 1
fi
if [[ "$count" != "$expected_count" ]]; then
  echo "Got count $count, expecting $expected_count"
  exit 1
fi
echo "Mongo db query returned $count records, as expected"
exit 0

