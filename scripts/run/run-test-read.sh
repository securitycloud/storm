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


$CUR_DIR/recreate-topic.sh $TESTING_TOPIC 1 $KAFKA_CONSUMER
$CUR_DIR/log-to-service-topic.sh "Type=read, Topology=$TOPOLOGY, Computers=$COMPUTERS, Partitions=$PARTITIONS"
$CUR_DIR/run-topology.sh $TOPOLOGY $COMPUTERS true
$CUR_DIR/done-test.sh
$CUR_DIR/kill-topology.sh $TOPOLOGY
