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
if [ "$REMOTEHOST" = "" ]; then
  echo "Missing required remote host (-r)"
  exit
fi

#set -x

for keyspacename in $DBS
do
  echo `date +%Y-%m-%dT%H:%M:%S`" Snapshoting..."
  nodetool snapshot $keyspacename
  echo `date +%Y-%m-%dT%H:%M:%S`" Done. Moving snapshots to backup..."
  for keyspacepath in $BASEPATH/$keyspacename/*
  do
    cd $keyspacepath
    for snap in $keyspacepath*/snapshots/*
    do
      if [[ $snap =~ snapshots\/[0-9]{13}$ ]]; then
        tablename=$(echo "$snap" | sed -r 's/.*\/(.*)-[a-f0-9]{32}\/snapshots\/[0-9]{13}/\1/')
        snapshot_timestamp=$(echo "$snap" | sed -r 's/.*\/.*-[a-f0-9]{32}\/snapshots\/([0-9]{13})/\1/')
        echo `date +%Y-%m-%dT%H:%M:%S`" snap folder:[$snap] tablename:[$tablename] snapshot_timestamp:[$snapshot_timestamp]"
        REMOTEFULLPATH="$REMOTEFOLDER/`hostname`/$BACKUPDATE/$keyspacename"
        cd $(dirname $snap)
        if [ ! -d "$tablename" ]; then
          echo `date +%Y-%m-%dT%H:%M:%S`" Move [$snapshot_timestamp] to [$tablename]"
          mv "$snapshot_timestamp" "$tablename"
          echo `date +%Y-%m-%dT%H:%M:%S`" tar -cf - $tablename | ssh $REMOTEHOST 'bzip2 > $REMOTEFULLPATH/$tablename.tbz2'"
          ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATH"
          tar -cf - $tablename | ssh $REMOTEHOST "bzip2 > $REMOTEFULLPATH/$tablename.tbz2"
          rm -rf $snap
          rm -rf $tablename
        else
          echo `date +%Y-%m-%dT%H:%M:%S`" Abort, folder:[$tablename] already exists"
        fi
      fi
    done
  done
  echo `date +%Y-%m-%dT%H:%M:%S`" Done"
done

if [ ! $NOTIFYFILE = "" ]; then
  echo `date +%Y-%m-%dT%H:%M:%S`" Notify backup server with file [$NOTIFYFILE]"
  ssh $REMOTEHOST "echo $BACKUPDATE > $NOTIFYFILE"
fi

echo `date +%Y-%m-%dT%H:%M:%S`" ALL DONE"
