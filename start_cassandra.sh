#!/bin/bash

pgrep -f org.apache.cassandra.service.CassandraDaemon >/dev/null

if [ $? == 1 ]; then
  echo "cassandra not started"
  service cassandra start
fi

