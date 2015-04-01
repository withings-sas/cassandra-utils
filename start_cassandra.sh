#!/bin/bash

#pgrep -f org.apache.cassandra.service.CassandraDaemon >/dev/null # not working with long cmd line
pgrep -f 'Dcassandra-pidfile=/var/run/cassandra/cassandra.pid' >/dev/null

if [ $? == 1 ]; then
  echo "cassandra not started"
  service cassandra start
fi

