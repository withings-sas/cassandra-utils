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

echo `date +%Y-%m-%dT%H:%M:%S`" START [$DESTBASEPATH] [$DBS] [$REMOTETARGET]"

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
  echo `date +%Y-%m-%dT%H:%M:%S`" Snapshoting..."
  nodetool snapshot $DB
  echo `date +%Y-%m-%dT%H:%M:%S`" Done. Moving snapshots to backup..."
  for keyspace in $BASEPATH/$DB/*
  do
    for snap in $keyspace*/snapshots/*
    do
      echo $snap | egrep 'snapshots\/[0-9]{13}$' >/dev/null
      if [ $? -eq 0 ]; then
        DESTFULLPATH=$DESTPATH/$(echo "$snap" | awk -F '/' '{ print $6"/"$7 }')
        echo `date +%Y-%m-%dT%H:%M:%S`"   MOVE [$snap] to [$DESTFULLPATH]"
        mkdir -p $DESTFULLPATH
        mv $snap/* $DESTFULLPATH
        rmdir $snap
      fi
    done
  done
  echo `date +%Y-%m-%dT%H:%M:%S`" Done"
done

if [ ! $REMOTETARGET = "" ]; then
  echo `date +%Y-%m-%dT%H:%M:%S`" Copy to [$REMOTETARGET]"
  rsync -az $DESTBASEPATH/$BACKUPDATE/ $REMOTETARGET/$BACKUPDATE/
  if [ $? -eq 0 ]; then
    echo `date +%Y-%m-%dT%H:%M:%S`" Cleanup [$DESTBASEPATH/$BACKUPDATE]"
    rm -rf "$DESTBASEPATH/$BACKUPDATE"
  fi
fi

echo `date +%Y-%m-%dT%H:%M:%S`" ALL DONE"
