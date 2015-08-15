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
$CUR_DIR/log-to-service-topic.sh "Type=readWrite, Topology=$TOPOLOGY, Computers=$COMPUTERS, Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE"
$CUR_DIR/test-partitions.sh $PARTITIONS
$CUR_DIR/run-topology.sh $TOPOLOGY $COMPUTERS false
$CUR_DIR/run-input.sh $BATCH_SIZE
$CUR_DIR/kill-topology.sh $TOPOLOGY
