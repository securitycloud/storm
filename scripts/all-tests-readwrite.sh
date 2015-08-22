#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

TOPOLOGIES[1]=TopologyKafkaKafka
TOPOLOGIES[2]=TopologyKafkaFilterKafka
TOPOLOGIES[3]=TopologyKafkaCounterKafka
TOPOLOGIES[4]=TopologyKafkaAggregationKafka
TOPOLOGIES[5]=TopologyKafkaTopNKafka
TOPOLOGIES[6]=TopologyKafkaTcpSynKafka

BATCH_SIZE[1]=1000
BATCH_SIZE[2]=5000

COMPUTERS[1]=1
COMPUTERS[2]=3
COMPUTERS[3]=5

REPEAT=5

NUM_TESTS=${#TOPOLOGIES[@]}
NUM_TESTS=$((NUM_TESTS * ${#BATCH_SIZE[@]}))
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
ACT_TEST=1

$CUR_DIR/clean/clean-cluster.sh
$CUR_DIR/install/install-cluster.sh
$CUR_DIR/start/start-cluster.sh
sleep 20
$CUR_DIR/run/begin/recreate-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for BS in "${BATCH_SIZE[@]}"
do
    for PC in "${COMPUTERS[@]}"
    do
        for i in `seq 1 $REPEAT`
        do
            for TP in "${TOPOLOGIES[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                $CUR_DIR/run/begin/run-test-readwrite.sh $TP $PC $PC $BS
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done

echo -e $LOG Downloading and parsing results $OFF
OUT=out.`date +%s`
$CUR_DIR/run/end/result-download.sh > $OUT.orig
$CUR_DIR/run/end/result-parse.sh $OUT.orig > $OUT.parsed
