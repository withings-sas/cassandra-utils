#!/bin/bash

# To restore an rdiff-backup increment:
# 1) list increment with 
#	rdiff-backup -l $REMOTE_HOST::$REMOTE_PATH
#	ex :
#	rdiff-backup -l backup@fr-hq-bkp-01::/data/backup-eqx-cas/rdiffbackup/fr-eqx-cas-09
#		Found 4 increments:
#		    increments.2016-07-16T17:55:23+02:00.dir   Sat Jul 16 17:55:23 2016       # <== let's restore this one
#		    increments.2016-07-17T17:35:00+02:00.dir   Sun Jul 17 17:35:00 2016
#		    increments.2016-07-18T16:32:02+02:00.dir   Mon Jul 18 16:32:02 2016
#		    increments.2016-07-19T16:32:03+02:00.dir   Tue Jul 19 16:32:03 2016
#		Current mirror: Wed Jul 20 17:31:03 2016
#
# 2) call the script without option -t and use -i with an increment date
#  ./restore.sh -s fr-eqx-cas-09 -k 'campaign vasistas' -r backup@fr-hq-bkp-01 -f /data/backup-eqx-cas/rdiffbackup -m rdiff-backup -i "2016-07-16T17:55:23+02:00"


PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

while getopts "s:d:t:k:r:f:m:v:i:" opt; do
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
    m)
      METHOD=$OPTARG
      ;;
    v)
      VIGILANTE_ID=$OPTARG
      ;;
    i)
      INCREMENT_DATE=$OPTARG
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
if [ "$BACKUP_DATE" = "" -a "$TRIGGER_FILE" = "" -a "$INCREMENT_DATE" = "" ]; then
  echo "Missing required date (-d), trigger file (-t) or increment date (-i)"
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

[ -n "$VIGILANTE_ID" ] && curl "http://vigilante.corp.withings.com/checkin/$VIGILANTE_ID?start" &> /dev/null

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
      if [ $METHOD = "rsync" ]; then
        BACKUP_FULLPATH="data/"$BACKUP_DATE"/"$keyspacename"/"$table"*"
      elif [ $METHOD = "rdiff-backup" ]; then
	BACKUP_FULLPATH=$( ssh $REMOTE_HOST "cd $REMOTE_PATH/$BACKUP_HOST; ls -d $keyspacename/$table*" )
      else
        BACKUP_FULLPATH=$BACKUP_DATE"/"$keyspacename"/"$table"*.tbz2"
      fi

      if [ ! -z $tablefullpath -a ! -z $table ]; then
        echo "table:[$table] method:[$METHOD] "$BACKUP_FULLPATH" TO "$tablefullpath
	MESSAGE+="Restoring $keyspacename:${table%-*}"$'\n'
        find "$tablefullpath/" -type f -delete
        if [ $METHOD = "rsync" ] || [ $METHOD = "rdiff-backup" -a -z "$INCREMENT_DATE" ]; then
          CMD="rsync -a --delete $REMOTE_HOST:$REMOTE_PATH/$BACKUP_HOST/$BACKUP_FULLPATH/ $tablefullpath/"
          echo "  "$CMD
          $CMD
        elif [ $METHOD = "rdiff-backup" ] && [ -n "$INCREMENT_DATE" ]; then # restore increment
          CMD="rdiff-backup --force -r $INCREMENT_DATE $REMOTE_HOST::$REMOTE_PATH/$BACKUP_HOST/$BACKUP_FULLPATH/ $tablefullpath/"
          echo "  "$CMD
          $CMD
        else
          ssh $REMOTE_HOST "cat $REMOTE_PATH/$BACKUP_HOST/$BACKUP_FULLPATH" | tar -C "$tablefullpath" -xjf -
        fi
        chown cassandra: -R $tablefullpath
      fi
    fi
  fi
done

for keyspacename in $DBS; do
  if [ $keyspacename = "system" ]; then
    echo "Skip keyspace [$keyspacename]"
    continue
  fi
  if [ $METHOD = "rsync" ]; then
    ksremotefullpath=$REMOTE_PATH"/"$BACKUP_HOST"/data/"$BACKUP_DATE"/"$keyspacename
  elif [ $METHOD = "rdiff-backup" ]; then
    ksremotefullpath=$REMOTE_PATH"/"$BACKUP_HOST"/"$keyspacename
  else
    ksremotefullpath=$REMOTE_PATH"/"$BACKUP_HOST"/"$BACKUP_DATE"/"$keyspacename
  fi

  ssh $REMOTE_HOST "stat $ksremotefullpath >/dev/null 2>&1"
  if [ ! $METHOD = "rsync" ] && [ ! $METHOD = "rdiff-backup" ]; then
    if [ $? -eq 0 ]; then
      # There is backup available for this KS, purge it
      echo "Delete $keyspacename data"
      rm -rf /var/lib/cassandra/data/$keyspacename
    fi
  fi

  cfs=$(ssh $REMOTE_HOST "ls $ksremotefullpath/")
  for cf in $cfs; do
    cf_ext="${cf##*.}"
    cf_name="${cf%.*}"
    #echo "CF:"$cf_name" EXT:"$cf_ext
    tablefullpath=/var/lib/cassandra/data/$keyspacename/$cf_name
    mkdir -p $tablefullpath
    MESSAGE+="Restoring $keyspacename:$cf_name"$'\n'
    if [ $METHOD = "rsync" ] || [ $METHOD = "rdiff-backup" -a -z "$INCREMENT_DATE" ]; then
      echo "Copy $ksremotefullpath/$cf into $tablefullpath"
      CMD="rsync -a --delete $REMOTE_HOST:$ksremotefullpath/$cf/ $tablefullpath/"
      echo "  "$CMD
      $CMD
    elif [ $METHOD = "rdiff-backup" ] && [ -n "$INCREMENT_DATE" ]; then # restore increment
      CMD="rdiff-backup --force -r $INCREMENT_DATE $REMOTE_HOST::$ksremotefullpath/$cf/ $tablefullpath/"
      echo "  "$CMD
      $CMD
    else
      if [ $cf_ext = "tbz2" ]; then
        echo "Extract $ksremotefullpath/$cf into $tablefullpath"
        ssh $REMOTE_HOST "cat $ksremotefullpath/$cf" | tar -C "$tablefullpath" -xjf -
      fi
    fi
    chown cassandra: -R $tablefullpath
  done
done

rm -rf /var/lib/cassandra/commitlog/*
rm -rf /var/lib/cassandra/saved_caches/*

echo "Starting cassandra..."
service cassandra start
sleep 10
echo "Disabling auto compaction..."
nodetool disableautocompaction
echo "All done"

#pgrep -f org.apache.cassandra.service.CassandraDaemon > /dev/null
#STATUS=$?
STATUS=0

[ -n "$VIGILANTE_ID" ] && curl --data "status=$STATUS&message=$MESSAGE" http://vigilante.corp.withings.com/checkin/$VIGILANTE_ID &> /dev/null

