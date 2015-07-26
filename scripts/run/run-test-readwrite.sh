#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Topology $OFF
    exit 1;
fi
TOPOLOGY=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify Number of computers $OFF
    exit 2;
fi
COMPUTERS=$2

if [ -z "$3" ] 
then
    echo -e $ERR You must specify Number of partitions $OFF
    exit 3;
fi
PARTITIONS=$3

if [ -z "$4" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 4;
fi
BATCH_SIZE=$4


$CUR_DIR/recreate-topic.sh $TESTING_TOPIC $PARTITIONS $KAFKA_PRODUCER
$CUR_DIR/recreate-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER

STORM_EXE=$WRK/storm/bin/storm
STORM_JAR=$WRK/project/target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar

echo -e $LOG Running topology $TOPOLOGY on $COMPUTERS computers $OFF
ssh root@$SRV_NIMBUS "
    $STORM_EXE jar $STORM_JAR cz.muni.fi.storm.$TOPOLOGY $COMPUTERS false
"

echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
ssh root@$KAFKA_CONSUMER "
    echo Type=readwrite, Topology=$TOPOLOGY, Computers=$COMPUTERS, Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE |
        $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
"

# TESTING CORRECT NUMBER OF PARTITIONS
ssh root@$KAFKA_PRODUCER "
    ls -la /tmp/kafka-logs/ | grep storm-test | wc -l > /tmp/storm-partitions
"
scp root@$KAFKA_PRODUCER:/tmp/storm-partitions /tmp/storm-partitions
REAL_PARTITIONS=`cat /tmp/storm-partitions`
if [ $REAL_PARTITIONS -ne $PARTITIONS ]
then
    ssh root@$KAFKA_CONSUMER "
        echo ERROR: exist $REAL_PARTITIONS partitions |
            $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
    "
fi

$CUR_DIR/run-input.sh $BATCH_SIZE

$CUR_DIR/kill-topology.sh $TOPOLOGY
