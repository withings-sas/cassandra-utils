#!/bin/bash

BACKUPDATE=$(date +%Y%m%d_%H%M%S)
BASEPATH=/var/lib/cassandra/data
TS_START=$(date +%s)

while getopts "k:p:r:f:n:m:v:" opt; do
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
    m)
      METHOD=$OPTARG
      ;;
    v)
      VIGILANTE_ID=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

echo `date +%Y-%m-%dT%H:%M:%S`" START Keyspaces:[$DBS] Remote:[$REMOTEHOST:$REMOTEFOLDER] Notify:[$NOTIFYFILE] Method:[$METHOD] VigilanteID:[$VIGILANTE_ID]"

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
  REMOTEFULLPATHSCHEMA="$REMOTEFOLDER/`hostname`/schema/$BACKUPDATE/$KEYSPACE"
  ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATHSCHEMA"
  echo "DESCRIBE KEYSPACE;" | cqlsh -k $KEYSPACE | ssh $REMOTEHOST "cat > $REMOTEFULLPATHSCHEMA/schema_$KEYSPACE.cql"
done

echo `date +%Y-%m-%dT%H:%M:%S`" Clear all snapshots..."
nodetool clearsnapshot

for keyspacename in $DBS
do
  if [ $METHOD = "rsync" ]; then
    REMOTEFULLPATH="$REMOTEFOLDER/`hostname`/incremental/$keyspacename"
  else
    REMOTEFULLPATH="$REMOTEFOLDER/`hostname`/$BACKUPDATE/$keyspacename"
  fi
  ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATH"
  echo `date +%Y-%m-%dT%H:%M:%S`" Snapshoting..."
  nodetool snapshot $keyspacename
  echo `date +%Y-%m-%dT%H:%M:%S`" Done. Moving snapshots to backup..."
  for keyspacepath in $BASEPATH/$keyspacename/*
  do
    cd $keyspacepath
    for snap in $keyspacepath*/snapshots/*
    do
      if [[ $snap =~ snapshots\/[0-9]{13}$ ]]; then
        columnfamily=$(echo "$snap" | sed -r 's/.*\/(.*-[a-f0-9]{32})\/snapshots\/[0-9]{13}/\1/')
        if [ $METHOD = "rsync" ]; then
          # rsync
          CMD="rsync -a $snap/ $REMOTEHOST:$REMOTEFULLPATH/$columnfamily/"
          echo `date +%Y-%m-%dT%H:%M:%S`" "$CMD
          MESSAGE+=$(date +%Y-%m-%dT%H:%M:%S)" Backuping ${columnfamily%-*}"$'\n'
          $CMD
        else
          # tar
          echo `date +%Y-%m-%dT%H:%M:%S`" tar -C $snap -cf - . | pbzip2 -p8 | ssh $REMOTEHOST 'cat > $REMOTEFULLPATH/$columnfamily.tbz2'"
          MESSAGE+="Backuping ${columnfamily%-*}"$'\n'
          tar -C "$snap" -cf - . | pbzip2 -p8 | ssh $REMOTEHOST "cat > $REMOTEFULLPATH/$columnfamily.tbz2"
        fi
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

if [ $METHOD = "rsync" ]; then
  size=$(ssh $REMOTEHOST "du -sh $REMOTEFOLDER/`hostname`/incremental")
else
  size=$(ssh $REMOTEHOST "du -sh $REMOTEFOLDER/`hostname`/$BACKUPDATE")
fi
echo Backup size $size

MESSAGE+=$'\n'$'\n'"Backup Size : "$size

if [ ! "$VIGILANTE_ID" = "" ]; then
  echo `date +%Y-%m-%dT%H:%M:%S`" Notify Vigilante"
  TS_END=$(date +%s)
  DURATION=$(( $TS_END - $TS_START ))

  echo "message="$MESSAGE > /tmp/vigilante.message

  curl --data "status=$STATUS&duration=$DURATION" --data @/tmp/vigilante.message http://vigilante.corp.withings.com/checkin/$VIGILANTE_ID &> /dev/null

  #rm /tmp/vigilante.message
fi

echo `date +%Y-%m-%dT%H:%M:%S`" ALL DONE"
