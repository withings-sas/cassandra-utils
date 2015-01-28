#!/bin/bash

BACKUPDATE=$(date +%Y%m%d_%H%M%S)
BASEPATH=/var/lib/cassandra/data

while getopts "k:p:r:f:n:" opt; do
  case $opt in
    p)
      BACKUP_TEMP_FOLDER=$OPTARG
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

echo `date +%Y-%m-%dT%H:%M:%S`" START BackupPath:[$BACKUP_TEMP_FOLDER] Keyspaces:[$DBS] Remote:[$REMOTEHOST:$REMOTEFOLDER] Notify:[$NOTIFYFILE]"

if [ "$DBS" = "" ]; then
  echo "Missing required keyspaces (-k)"
  exit
fi
if [ "$BACKUP_TEMP_FOLDER" = "" -o "$BACKUP_TEMP_FOLDER" = "/" ]; then
  echo "Missing required base path (-p)"
  exit
fi
if [ "$REMOTEHOST" = "" ]; then
  echo "Missing required remote host (-r)"
  exit
fi

BACKUP_TEMP_FOLDER=${BACKUP_TEMP_FOLDER%/}"/"

set -x

mkdir -p $BACKUP_TEMP_FOLDER
cd $BACKUP_TEMP_FOLDER

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
        echo `date +%Y-%m-%dT%H:%M:%S`" snap folder:[$snap] tablename:[$tablename]"
        REMOTEFULLPATH="$REMOTEFOLDER/`hostname`/$BACKUPDATE/$keyspacename"
        if [ ! -d "$BACKUP_TEMP_FOLDER$tablename" ]; then
          echo `date +%Y-%m-%dT%H:%M:%S`" Move [$snap] to [$BACKUP_TEMP_FOLDER$tablename]"
          mv "$snap" "$BACKUP_TEMP_FOLDER$tablename"
          ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATH"
          echo `date +%Y-%m-%dT%H:%M:%S`" tar -cf - $tablename | ssh $REMOTEHOST 'pbzip2 -p2 > $REMOTEFULLPATH/$tablename.tbz2'"
          cd $BACKUP_TEMP_FOLDER
          tar -cf - "$tablename" | ssh $REMOTEHOST "pbzip2 -p2 > $REMOTEFULLPATH/$tablename.tbz2"
          rm -rf "$BACKUP_TEMP_FOLDER$tablename"
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
