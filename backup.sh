#!/bin/bash

BACKUPDATE=$(date +%Y%m%d_%H%M%S)
BASEPATH=/var/lib/cassandra/data

while getopts "k:p:r:" opt; do
  case $opt in
    p)
      DESTBASEPATH=$OPTARG
      ;;
    k)
      DBS=$OPTARG
      ;;
    r)
      REMOTETARGET=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

DESTPATH=$DESTBASEPATH"/"$BACKUPDATE"/"`hostname`

echo "[$DESTBASEPATH] [$DBS] [$REMOTETARGET]"

if [ "$DBS" = "" ]; then
  echo "Missing required keyspaces (-k)"
  exit
fi

if [ "$DESTBASEPATH" = "" -o "$DESTBASEPATH" = "/" ]; then
  echo "Missing required base path (-p)"
  exit
fi

#exit

for DB in $DBS
do
  echo "Snapshoting..."
  nodetool snapshot $DB
  echo "Done. Moving snapshots to backup..."
  for keyspace in $BASEPATH/$DB/*
  do
    for snap in $keyspace*/snapshots/*
    do
      echo $snap | egrep 'snapshots\/[0-9]{13}$' >/dev/null
      if [ $? -eq 0 ]; then
        DESTFULLPATH=$DESTPATH/$(echo "$snap" | awk -F '/' '{ print $6"/"$7 }')
        echo "  MOVE [$snap] to [$DESTFULLPATH]"
        mkdir -p $DESTFULLPATH
        mv $snap/* $DESTFULLPATH
        rmdir $snap
      fi
    done
  done
  echo "Done"
done

if [ ! $REMOTETARGET = "" ]; then
  echo "Copy to [$REMOTETARGET]"
  rsync -az $DESTBASEPATH/$BACKUPDATE/ $REMOTETARGET/$BACKUPDATE/
  if [ $? -eq 0 ]; then
    echo "Cleanup [$DESTBASEPATH/$BACKUPDATE]"
    rm -rf "$DESTBASEPATH/$BACKUPDATE"
  fi
fi
