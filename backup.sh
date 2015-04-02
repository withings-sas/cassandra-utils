#!/bin/bash

BACKUPDATE=$(date +%Y%m%d_%H%M%S)
BASEPATH=/var/lib/cassandra/data

while getopts "k:p:r:f:n:" opt; do
  case $opt in
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

echo `date +%Y-%m-%dT%H:%M:%S`" START Keyspaces:[$DBS] Remote:[$REMOTEHOST:$REMOTEFOLDER] Notify:[$NOTIFYFILE]"

if [ "$DBS" = "" ]; then
  echo "Missing required keyspaces (-k)"
  exit
fi
if [ "$REMOTEHOST" = "" ]; then
  echo "Missing required remote host (-r)"
  exit
fi

#set -x

# Backup schema
echo `date +%Y-%m-%dT%H:%M:%S`" Backup schema..."
for KEYSPACE in $(echo "DESCRIBE KEYSPACES;" | cqlsh | sed '/^$/d' | xargs); do
  echo `date +%Y-%m-%dT%H:%M:%S`" Backup schema of keyspace [$KEYSPACE]..."
  REMOTEFULLPATHSCHEMA="$REMOTEFOLDER/`hostname`/$BACKUPDATE/$KEYSPACE"
  ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATHSCHEMA"
  echo "DESCRIBE KEYSPACE;" | cqlsh -k $KEYSPACE | ssh $REMOTEHOST "cat > $REMOTEFULLPATHSCHEMA/schema_$KEYSPACE.cql"
done

echo `date +%Y-%m-%dT%H:%M:%S`" Clear all snapshots..."
nodetool clearsnapshot

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
        ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATH"
        echo `date +%Y-%m-%dT%H:%M:%S`" tar -C $snap -cf - . | pbzip2 -p3 | ssh $REMOTEHOST 'cat > $REMOTEFULLPATH/$tablename.tbz2'"
        tar -C "$snap" -cf - . | pbzip2 -p2 | ssh $REMOTEHOST "cat > $REMOTEFULLPATH/$tablename.tbz2"
      fi
    done
  done
  echo `date +%Y-%m-%dT%H:%M:%S`" Done"
done

echo `date +%Y-%m-%dT%H:%M:%S`" Clear all snapshots..."
nodetool clearsnapshot

if [ ! $NOTIFYFILE = "" ]; then
  echo `date +%Y-%m-%dT%H:%M:%S`" Notify backup server with file [$NOTIFYFILE]"
  ssh $REMOTEHOST "echo $BACKUPDATE > $NOTIFYFILE"
fi

echo `date +%Y-%m-%dT%H:%M:%S`" ALL DONE"
