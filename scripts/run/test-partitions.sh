#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ]
then
    echo -e $ERR You must specify correct Number of partitions $OFF
    exit 1;
fi
PARTITIONS=$1

# SAVE ACTUAL NUMBER OF PARTITIONS AND DOWNLOAD IT
ssh root@$KAFKA_PRODUCER "
    ls -la /tmp/kafka-logs/ | grep storm-test | wc -l > /tmp/storm-partitions
"
scp root@$KAFKA_PRODUCER:/tmp/storm-partitions /tmp/storm-partitions


# COMPARE ACTUAL NUMBER OF PARTITIONS AGAINST INPUT 
REAL_PARTITIONS=`cat /tmp/storm-partitions`
if [ $REAL_PARTITIONS -ne $PARTITIONS ]
then
    # IF NOT, SEND LOG
    $CUR_DIR/log-to-service-topic.sh "ERROR: exist $REAL_PARTITIONS partitions"
    echo -e $ERR ERROR: exist $REAL_PARTITIONS partitions $OFF
    exit(1)
fi
