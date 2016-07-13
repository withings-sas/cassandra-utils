#!/bin/bash

function log() { echo $(date "+%F %T")" : $@"; }

if [ -n "$1" ] && [ "$1" = "--dry-run" ]; then
        dry_run=1
else
        dry_run=0
fi

for dir in $(find /var/lib/cassandra/data/vasistas/* -type d| grep -v snapshots); do
        subfolder=$(basename $dir)
        table=${subfolder%-*}
        size=$(du -sm $dir| cut -f1)
        nb_files=$(find $dir -type f | wc -l)

        [ $nb_files -lt 1000 ] && continue
        if [ $nb_files -gt 100000 ]; then
                log "     * $table has more than 100k files : not compacting automatically"
                continue
        fi
        log "Compacting $table : $size Mo / $nb_files files"
        [ "$dry_run" -eq 0 ] && nodetool compact vasistas $table
done
log "Done"

