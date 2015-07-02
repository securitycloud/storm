#!/bin/bash

. scripts/setenv.sh

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

# DELETE AND CREATE TOPIC
ssh root@$SERVER "
    $KAFKA_INSTALL/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $TOPIC
    $KAFKA_INSTALL/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC
"
