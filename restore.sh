#!/bin/bash

BACKUP_PATH=/var/lib/cassandra/backup_data
CLEANUP="no"

while getopts "s:d:t:k:r:f:c:" opt; do
  case $opt in
    s)
      BACKUP_HOST=$OPTARG
      ;;
    d)
      BACKUP_DATE=$OPTARG
      ;;
    t)
      TRIGGER_FILE=$OPTARG
      ;;
    k)
      DBS=$OPTARG
      ;;
    r)
      REMOTE_HOST=$OPTARG
      ;;
    f)
      REMOTE_PATH=$OPTARG
      ;;
    c)
      CLEANUP=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

if [ "$BACKUP_HOST" = "" ]; then
  echo "Missing required source host (-s)"
  exit
fi
if [ "$BACKUP_DATE" = "" -a "$TRIGGER_FILE" = "" ]; then
  echo "Missing required date (-d) or trigger file (-t)"
  exit
fi
if [ "$DBS" = "" ]; then
  echo "Missing required keyspaces (-k)"
  exit
fi
if [ "$REMOTE_HOST" = "" ]; then
  echo "Missing required remote host (-r)"
  exit
fi
if [ "$REMOTE_PATH" = "" ]; then
  echo "Missing required remote path (-f)"
  exit
fi

if [ ! -z "$TRIGGER_FILE" ]; then
  echo "Checking trigger file [$TRIGGER_FILE]"
  ssh $REMOTE_HOST "stat $TRIGGER_FILE >/dev/null 2>&1"
  if [ $? -eq 0 ]; then
    BACKUP_DATE=$(ssh $REMOTE_HOST "cat $TRIGGER_FILE")
    ssh $REMOTE_HOST "rm $TRIGGER_FILE"
    echo "Found trigger file with content:[$BACKUP_DATE]"
  else
    echo "Trigger file not found"
    exit
  fi
fi

#mkdir -p "$BACKUP_PATH/$BACKUP_DATE/"
#SOURCE_FULLPATH="$REMOTE_HOST:$REMOTE_PATH/$BACKUP_HOST/$BACKUP_DATE/"
#echo "rsync from [$SOURCE_FULLPATH] to [$BACKUP_PATH]"
#for keyspacename in $DBS; do
#  rsync -az --progress $SOURCE_FULLPATH$keyspacename"/" "$BACKUP_PATH/$BACKUP_DATE/$keyspacename/"
#  if [ $? -ne 0 ]; then
#    echo "Error on rsync"
#    exit
#  fi
#done

# Real reload
service cassandra stop
sleep 1
pidof java && killall java

# Restore system tables (only a few)
keyspacename="system"
for tablefullpath in /var/lib/cassandra/data/$keyspacename/*; do
  tablepath=`basename $tablefullpath`
  if [[ $tablepath =~ [a-z0-9_-]+-[a-f0-9]{32} ]]; then
    table=$(echo $tablepath | sed -r 's/([a-z0-9_-]+)-[a-f0-9]{32}/\1/')
    if [ $table = "schema_columnfamilies" -o $table = "schema_columns" ]; then
      echo "will restore table [$table]"
      #BACKUP_FULLPATH=$BACKUP_PATH"/"$BACKUP_DATE"/"$keyspacename"/"$table".tbz2"
      #if [ -f $BACKUP_FULLPATH ]; then
      if [ ! -z $tablefullpath -a ! -z $table ]; then
          echo "table:[$table] "$BACKUP_FULLPATH" TO "$tablefullpath
          find "$tablefullpath/" -type f -delete
          ssh $REMOTE_HOST "cat $REMOTE_PATH/$BACKUP_HOST/$BACKUP_DATE/$keyspacename/$table.tbz2" | tar -C "$tablefullpath" -xjf -
      fi
    else
      echo "do not restore table [$table]"
    fi
  fi
done

rm -rf /var/lib/cassandra/commitlog/*
rm -rf /var/lib/cassandra/saved_caches/*
service cassandra start

# Wait for cassandra to start
sleep 5

for keyspacename in $DBS; do
  for tablefullpath in /var/lib/cassandra/data/$keyspacename/*; do
    tablepath=`basename $tablefullpath`
    if [[ $tablepath =~ [a-z0-9_-]+-[a-f0-9]{32} ]]; then
      table=$(echo $tablepath | sed -r 's/([a-z0-9_-]+)-[a-f0-9]{32}/\1/')
      #BACKUP_FULLPATH=$BACKUP_PATH"/"$BACKUP_DATE"/"$keyspacename"/"$table".tbz2"
      #if [ -f $BACKUP_FULLPATH ]; then
      if [ ! -z $tablefullpath -a ! -z $table ]; then
          echo "table:[$table] "$BACKUP_FULLPATH" TO "$tablefullpath
          find "$tablefullpath/" -type f -delete
          ssh $REMOTE_HOST "cat $REMOTE_PATH/$BACKUP_HOST/$BACKUP_DATE/$keyspacename/$table.tbz2" | tar -C "$tablefullpath" -xjf -
      fi
    fi
  done
  # Loads newly placed SSTables
  nodetool refresh
done

#rm -rf /var/lib/cassandra/commitlog/*
#rm -rf /var/lib/cassandra/saved_caches/*
#service cassandra start

if [ $CLEANUP = "yes" ]; then
  echo "Final cleanup of [$BACKUP_PATH/$BACKUP_DATE]"
  rm -rf "$BACKUP_PATH/$BACKUP_DATE"
fi
