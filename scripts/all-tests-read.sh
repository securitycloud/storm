#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

TOPOLOGIES[1]=TopologyKafkaKafka
TOPOLOGIES[2]=TopologyKafkaFilterKafka
TOPOLOGIES[3]=TopologyKafkaCounterKafka
TOPOLOGIES[4]=TopologyKafkaAggregationKafka
TOPOLOGIES[5]=TopologyKafkaTopNKafka

COMPUTERS[1]=1
COMPUTERS[2]=3
COMPUTERS[3]=5

REPEAT=5

NUM_TESTS=${#TOPOLOGIES[@]}
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
NUM_TESTS=$((NUM_TESTS * ${REPEAT}))
ACT_TEST=1

$CUR_DIR/clean/clean-cluster.sh
$CUR_DIR/install/install-cluster.sh
$CUR_DIR/start/start-cluster.sh
$CUR_DIR/run/begin/recreate-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for PC in "${COMPUTERS[@]}"
do
    $CUR_DIR/run/begin/log-to-service-topic.sh "Input topic for read tests: Partitions=$PC"
    $CUR_DIR/run/begin/recreate-topic.sh $TESTING_TOPIC $PC $KAFKA_PRODUCER
    $CUR_DIR/run/begin/run-input.sh 5000

    for i in `seq 1 $REPEAT`
    do
        for TP in "${TOPOLOGIES[@]}"
        do
            echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
            $CUR_DIR/run/run-test-read.sh $TP $PC $PC
            ACT_TEST=$((ACT_TEST + 1))
        done
    done
done

echo -e $LOG Downloading and parsing results $OFF
$CUR_DIR/run/end/result-download.sh | $CUR_DIR/run/end/result-parse.sh > out.`date +%s`.txt
