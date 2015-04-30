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

if [[ ! `hostname` == *"casbkp"* ]]; then
  echo "not a backup machine, do not run this script here"
  exit 1
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
    if [ $table = "schema_columnfamilies" -o $table = "schema_columns" -o $table = "schema_keyspaces" ]; then
      echo "will restore table [$table]"
      BACKUP_FULLPATH=$BACKUP_DATE"/"$keyspacename"/"$table"*.tbz2"
      #if [ -f $BACKUP_FULLPATH ]; then
      if [ ! -z $tablefullpath -a ! -z $table ]; then
          echo "table:[$table] "$BACKUP_FULLPATH" TO "$tablefullpath
          find "$tablefullpath/" -type f -delete
          ssh $REMOTE_HOST "cat $REMOTE_PATH/$BACKUP_HOST/$BACKUP_FULLPATH" | tar -C "$tablefullpath" -xjf -
      fi
    #else
    #  echo "do not restore table [$table]"
    fi
  fi
done

for keyspacename in $DBS; do
  if [ $keyspacename = "system" ]; then
    echo "Skip keyspace [$keyspacename]"
    continue
  fi
  ksremotefullpath=$REMOTE_PATH/$BACKUP_HOST/$BACKUP_DATE/$keyspacename

  ssh $REMOTE_HOST "stat $ksremotefullpath >/dev/null 2>&1"
  if [ $? -eq 0 ]; then
    # There is backup available for this KS, purge it
    echo "Delete $keyspacename data"
    rm -rf /var/lib/cassandra/data/$keyspacename
  fi

  cfs=$(ssh $REMOTE_HOST "ls $ksremotefullpath/")
  for cf in $cfs; do
    cf_ext="${cf##*.}"
    cf_name="${cf%.*}"
    #echo "CF:"$cf_name" EXT:"$cf_ext
    if [ $cf_ext = "tbz2" ]; then
      tablefullpath=/var/lib/cassandra/data/$keyspacename/$cf_name
      mkdir -p $tablefullpath
      echo "Extract $ksremotefullpath/$cf into $tablefullpath"
      ssh $REMOTE_HOST "cat $ksremotefullpath/$cf" | tar -C "$tablefullpath" -xjf -
      chown cassandra:cassandra -R $tablefullpath
    fi
  done
  # Loads newly placed SSTables
  #nodetool refresh $keyspacename $table
done

rm -rf /var/lib/cassandra/commitlog/*
rm -rf /var/lib/cassandra/saved_caches/*

echo "Starting cassandra..."
service cassandra start
sleep 10
echo "Disabling auto compaction..."
nodetool disableautocompaction
echo "All done"

#if [ $CLEANUP = "yes" ]; then
#  echo "Final cleanup of [$BACKUP_PATH/$BACKUP_DATE]"
#  rm -rf "$BACKUP_PATH/$BACKUP_DATE"
#fi
