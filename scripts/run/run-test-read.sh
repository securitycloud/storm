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


$CUR_DIR/begin/recreate-topic.sh $OUTPUT_TOPIC 1 $KAFKA_CONSUMER
$CUR_DIR/begin/log-to-service-topic.sh "Type=read, Topology=$TOPOLOGY, Computers=$COMPUTERS, Partitions=$PARTITIONS"
$CUR_DIR/begin/run-topology.sh $TOPOLOGY $COMPUTERS true
$CUR_DIR/end/done-test.sh
$CUR_DIR/end/kill-topology.sh $TOPOLOGY
