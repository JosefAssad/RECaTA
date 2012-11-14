#!/bin/sh

BACKUP_PATH="../data/"
#TABLES="cities dataruns"
TABLES="cities listings listingdata datapages dataruns"
DB_NAME="boligadata"
DB_USER="boligadata"
TIMESTAMP=`date +%Y%m%d-%H%M%S`

for TABLE in $TABLES
do
    mkdir -p $BACKUP_PATH/tabledumps-$TIMESTAMP
    echo "dumping data for table " $TABLE
    pg_dump -U $DB_USER --no-password --data-only \
	--table=$TABLE $DB_NAME > \
	$BACKUP_PATH/tabledumps-$TIMESTAMP/$TABLE.sql
done
