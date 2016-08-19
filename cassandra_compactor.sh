#!/bin/bash

set -u

dry_run=0
repair=0
max_nb_sstables=10000
table_pattern=".*"

while getopts "k:t:m:dr" opt; do
  case $opt in
    k)
      keyspace=$OPTARG
      ;;
    m)
      max_sstables=$OPTARG
      ;;
    t)
      table_pattern=$OPTARG
      ;;
    d)
      dry_run=1
      ;;
    r)
      repair=1
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

function log() { echo $(date "+%F %T")" : $@"; }

if [ -z $keyspace ]; then
	echo "Missing keyspace argument"
	echo "Usage: $0 -k <keyspace> [-d] [-r]"
        exit 1
fi

log "Start, keyspace=$keyspace dry_run=$dry_run repair=$repair"
for dir in $(find /var/lib/cassandra/data/$keyspace/* -type d| grep -v snapshots); do
        subfolder=$(basename $dir)
        table=${subfolder%-*}
        size=$(du -sm $dir| cut -f1)
        nb_sstables=$(find $dir -type f -name *Data.db| wc -l)

	if [[ $table =~ ^$table_pattern$ ]]; then
        	log "  table $table nb_sstables=$nb_sstables size=${size}Mo, $(( $size/ $nb_sstables))Mo/sstable"
	else
		continue
	fi

        [ "$dry_run" -eq 0 -a "$repair" -eq 1 ] && nodetool repair $keyspace $table

        [ $nb_sstables -lt 100 ] && continue
        if [ $nb_sstables -gt $max_nb_sstables ]; then
                log "     * $table has more than $max_nb_sstables sstables : not compacting automatically"
                continue
        fi
        log "Compacting $table : $size Mo / $nb_sstables sstables"
        [ "$dry_run" -eq 0 ] && nodetool compact $keyspace $table
done
log "Done"

