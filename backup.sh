#!/bin/bash

BACKUPDATE=$(date +%Y%m%d_%H%M%S)
BASEPATH=/var/lib/cassandra/data

while getopts "k:p:r:f:n:" opt; do
  case $opt in
    p)
      DESTBASEPATH=$OPTARG
      ;;
    k)
      DBS=$OPTARG
      ;;
    r)
      REMOTEHOST=$OPTARG
      ;;
    f)
      REMOTEFOLDER=$OPTARG
      ;;
    n)
      NOTIFYFILE=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

DESTPATH=$DESTBASEPATH"/"$BACKUPDATE"/"`hostname`

echo `date +%Y-%m-%dT%H:%M:%S`" START LocalPath:[$DESTBASEPATH] Keyspaces:[$DBS] Remote:[$REMOTEHOST:$REMOTEFOLDER] Notify:[$NOTIFYFILE]"

if [ "$DBS" = "" ]; then
  echo "Missing required keyspaces (-k)"
  exit
fi

if [ "$DESTBASEPATH" = "" -o "$DESTBASEPATH" = "/" ]; then
  echo "Missing required base path (-p)"
  exit
fi

for DB in $DBS
do
  echo `date +%Y-%m-%dT%H:%M:%S`" Snapshoting..."
  nodetool snapshot $DB
  echo `date +%Y-%m-%dT%H:%M:%S`" Done. Moving snapshots to backup..."
  for keyspace in $BASEPATH/$DB/*
  do
    for snap in $keyspace*/snapshots/*
    do
      #echo $snap | egrep 'snapshots\/[0-9]{13}$' >/dev/null
      #if [ $? -eq 0 ]; then
      if [[ $snap =~ snapshots\/[0-9]{13}$ ]]; then
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

if [ ! $REMOTEHOST = "" ]; then
  echo `date +%Y-%m-%dT%H:%M:%S`" Copy from [$DESTBASEPATH/$BACKUPDATE/] to [$REMOTEHOST:$REMOTEFOLDER/`hostname`/$BACKUPDATE/]"
  ssh $REMOTEHOST "mkdir -p $REMOTEFOLDER/`hostname`/$BACKUPDATE/"
  rsync -az $DESTBASEPATH/$BACKUPDATE/ "$REMOTEHOST:$REMOTEFOLDER/`hostname`/$BACKUPDATE/"
  if [ $? -eq 0 ]; then
    echo `date +%Y-%m-%dT%H:%M:%S`" Cleanup [$DESTBASEPATH/$BACKUPDATE]"
    rm -rf "$DESTBASEPATH/$BACKUPDATE"
    if [ ! $NOTIFYFILE = "" ]; then
      echo `date +%Y-%m-%dT%H:%M:%S`" Notify backup server with file [$NOTIFYFILE]"
      ssh $REMOTEHOST "echo $BACKUPDATE > $NOTIFYFILE"
    fi
  fi
fi

echo `date +%Y-%m-%dT%H:%M:%S`" ALL DONE"
