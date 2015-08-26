#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify topic $OFF
    exit 1;
fi
TOPIC=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify number of partitions $OFF
    exit 2;
fi
PARTITIONS=$2

if [ -z "$3" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 3;
fi
SERVER=$3

# LOG
echo -e $LOG Recreating topic $TOPIC with $PARTITIONS partitions on $SERVER $OFF

# UNTIL TOPIS HAS BEEN CREATED WITH CORRECT NUMBER OF PARTITIONS
while [ true ]
do
    # DELETE AND CREATE TOPIC
    ssh $SERVER "
        $KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC
        $KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC
    "

    sleep 1

    # SAVE ACTUAL NUMBER OF PARTITIONS AND DOWNLOAD IT
    ssh $SERVER "
        ls -la /tmp/kafka-logs/ | grep $TOPIC | wc -l > /tmp/storm-partitions
    "
    scp $SERVER:/tmp/storm-partitions /tmp/storm-partitions

    # COMPARE ACTUAL NUMBER OF PARTITIONS AGAINST INPUT 
    REAL_PARTITIONS=`cat /tmp/storm-partitions`
    if [ $REAL_PARTITIONS -eq $PARTITIONS ]; then break; fi
done
