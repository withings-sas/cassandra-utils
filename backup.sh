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

for keyspacename in $DBS
do
  echo `date +%Y-%m-%dT%H:%M:%S`" Snapshoting..."
  nodetool snapshot $keyspacename
  echo `date +%Y-%m-%dT%H:%M:%S`" Done. Moving snapshots to backup..."
  for keyspacepath in $BASEPATH/$keyspacename/*
  do
    for snap in $keyspacepath*/snapshots/*
    do
      if [[ $snap =~ snapshots\/[0-9]{13}$ ]]; then
        #DESTFULLPATH=$DESTPATH/$(echo "$snap" | awk -F '/' '{ print $6"/"$7 }')
        tablename=$(echo "$snap" | sed -r 's/.*\/(.*)-[a-f0-9]{32}\/snapshots\/[0-9]{13}/\1/')
        DESTFULLPATH="$DESTPATH/$keyspacename/$tablename"
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
  for keyspacename in $DBS
  do
    if [ -d $DESTPATH/$keyspacename ]; then
      echo `date +%Y-%m-%dT%H:%M:%S`" cd $DESTPATH/$keyspacename"
      cd $DESTPATH/$keyspacename
      REMOTEFULLPATH="$REMOTEFOLDER/`hostname`/$BACKUPDATE/$keyspacename"
      ssh $REMOTEHOST "mkdir -p $REMOTEFULLPATH"
      for table in *
      do
        echo `date +%Y-%m-%dT%H:%M:%S`" tar -cf - $table | ssh $REMOTEHOST 'gzip > $REMOTEFULLPATH/$table.tgz'"
        tar -cf - $table | ssh $REMOTEHOST "gzip > $REMOTEFULLPATH/$table.tgz"
      done
      if [[ $DESTPATH =~ .*/tmp/.* ]]; then
        echo `date +%Y-%m-%dT%H:%M:%S`" rm -rf $DESTPATH/$keyspacename"
        rm -rf $DESTPATH/$keyspacename
      fi
    fi
  done

  #rsync -az $DESTBASEPATH/$BACKUPDATE/ "$REMOTEHOST:$REMOTEFOLDER/`hostname`/$BACKUPDATE/"
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
