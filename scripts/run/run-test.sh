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
    echo -e $ERR You must specify Number of parallelism $OFF
    exit 3;
fi
PARALLELISM=$3


ssh sc6 "cd ~/ekafsender; ./reset_kafka_topics.sh"
$CUR_DIR/begin/run-topology.sh $TOPOLOGY $COMPUTERS $PARALLELISM
sleep 10
ssh sc6 "cd ~/ekafsender/; ./run.sh"
$CUR_DIR/end/kill-topology.sh $TOPOLOGY
