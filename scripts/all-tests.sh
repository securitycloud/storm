#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

TOPOLOGIES[1]=TopologyEmpty
TOPOLOGIES[2]=TopologyFilter
TOPOLOGIES[3]=TopologyCounter
TOPOLOGIES[4]=TopologyAggregation
TOPOLOGIES[5]=TopologyTopN
TOPOLOGIES[6]=TopologyKafkaTcpSynKafka

COMPUTERS[1]=3
COMPUTERS[2]=4
COMPUTERS[3]=5

PARALLELISM[1]=20

REPEAT=5

NUM_TESTS=${#TOPOLOGIES[@]}
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
NUM_TESTS=$((NUM_TESTS * ${#PARALLELISM[@]}))
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
ACT_TEST=1

$CUR_DIR/clean/clean-cluster.sh
$CUR_DIR/install/install-cluster.sh
$CUR_DIR/start/start-cluster.sh
sleep 10

for PC in "${COMPUTERS[@]}"
do
    for i in `seq 1 $REPEAT`
    do
        for TP in "${TOPOLOGIES[@]}"
        do
            for PR in "${PARALLELISM[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                $CUR_DIR/run/run-test.sh $TP $PC $PR
                ACT_TEST=$((ACT_TEST + 1))
             done
        done
    done
done

echo -e $LOG Downloading and parsing results $OFF
OUT=out.`date +%s`
$CUR_DIR/run/end/result-download.sh > $OUT.orig
$CUR_DIR/run/end/result-parse.sh $OUT.orig > $OUT.parsed
