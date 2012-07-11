#!/bin/bash

search_term=$1
expected_count=$2
matches=`curl -s "http://127.0.0.1:8983/solr/select/?q=${search_term}&version=2.2&start=0&rows=10&indent=on" | grep numFound | awk '{print $3}' | python -c "import re;import sys; print re.search(r'\d+', sys.stdin.read()).group(0)"`
if [[ "$matches" == "$expected_count" ]]; then
  echo "Got correct number of search hits: $matches"
  exit 0
else
  echo "Got wrong number of search hits: expected $expected_count, got $matches"
  exit 1
fi
