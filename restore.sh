#!/bin/bash

BACKUP_PATH=/var/lib/cassandra/backup_data

while getopts "s:d:k:r:f:" opt; do
  case $opt in
    s)
      BACKUP_HOST=$OPTARG
      ;;
    d)
      BACKUP_DATE=$OPTARG
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
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

if [ "$BACKUP_HOST" = "" ]; then
  echo "Missing required source host (-s)"
  exit
fi
if [ "$BACKUP_DATE" = "" ]; then
  echo "Missing required date (-d)"
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

mkdir $BACKUP_PATH
SOURCE_FULLPATH="$REMOTE_HOST:$REMOTE_PATH/$BACKUP_HOST/`date +%Y%m%d`*"
echo "rsync from [$SOURCE_FULLPATH] to [$BACKUP_PATH]"
rsync -az $SOURCE_FULLPATH $BACKUP_PATH
if [ $? -ne 0 ]; then
  echo "Error on rsync"
  exit
fi

cd $BACKUP_PATH
echo "untar [`date +%Y%m%d`*]"
tar xjf `date +%Y%m%d`*.tbz2
if [ $? -ne 0 ]; then
  echo "Error on untar"
  exit
fi

# Real reload
#service cassandra stop

for DB in $DBS; do
  for tablefullpath in /var/lib/cassandra/data/$DB/*; do
    tablepath=`basename $tablefullpath`
    if [[ $tablepath =~ [a-z0-9_-]+-[a-f0-9]{32} ]]; then
      table=$(echo $tablepath | sed -r 's/([a-z0-9_-]+)-[a-f0-9]{32}/\1/')
      BACKUP_FULLPATH=$BACKUP_PATH"/backup/cassandra/"$BACKUP_HOST"/"$BACKUP_DATE"/"$BACKUP_HOST"/"$DB"/"$table"-*"
      if [ -d $BACKUP_FULLPATH ]; then
        echo $BACKUP_FULLPATH" TO "$tablefullpath
        #rsync -avz $BACKUP_FULLPATH/ $tablefullpath/
      fi
    fi
  done
done

#service cassandra start
