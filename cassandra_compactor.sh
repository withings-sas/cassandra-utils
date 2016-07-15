#!/bin/bash

dry_run=0
repair=0
max_nb_files=100000

while getopts "k:m:dr" opt; do
  case $opt in
    k)
      keyspace=$OPTARG
      ;;
    m)
      max_nb_files=$OPTARG
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

        [ "$dry_run" -eq 0 -a "$repair" -eq 1 ] && echo "nodetool repair $keyspace $table"

        size=$(du -sm $dir| cut -f1)
        nb_files=$(find $dir -type f | wc -l)
        log "  "$(basename $dir)" nb_files=$nb_files size=$size"

        [ $nb_files -lt 1000 ] && continue
        if [ $nb_files -gt $max_nb_files ]; then
                log "     * $table has more than $max_nb_files files : not compacting automatically"
                continue
        fi
        log "Compacting $table : $size Mo / $nb_files files"
        [ "$dry_run" -eq 0 ] && echo "nodetool compact $keyspace $table"
done
log "Done"

