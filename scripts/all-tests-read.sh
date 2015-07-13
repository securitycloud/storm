#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

TOPOLOGIES[1]=TopologyKafkaKafka
TOPOLOGIES[2]=TopologyKafkaFilterKafka

BATCH_SIZE[1]=1000
BATCH_SIZE[2]=5000

PARTITIONS[1]=1
PARTITIONS[2]=3
PARTITIONS[3]=5

COMPUTERS[1]=1
COMPUTERS[2]=2
COMPUTERS[3]=3
COMPUTERS[4]=4
COMPUTERS[5]=5

NUM_TESTS=${#TOPOLOGIES[@]}
NUM_TESTS=$((NUM_TESTS * ${#BATCH_SIZE[@]}))
NUM_TESTS=$((NUM_TESTS * ${#PARTITIONS[@]}))
NUM_TESTS=$((NUM_TESTS * ${#COMPUTERS[@]}))
ACT_TEST=1

$CUR_DIR/clean/clean-cluster.sh
$CUR_DIR/install/install-cluster.sh
$CUR_DIR/start/start-cluster.sh
$CUR_DIR/run/recreate-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for TP in "${TOPOLOGIES[@]}"
do
    for BS in "${BATCH_SIZE[@]}"
    do
        for PTN in "${PARTITIONS[@]}"
        do
            echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
            ssh root@$KAFKA_CONSUMER "
                echo Input topic for read tests: Partitions=$PARTITIONS, BatchSize=$BATCH_SIZE |
                    $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
            "

            $CUR_DIR/run/recreate-topic.sh $TESTING_TOPIC $PTN $KAFKA_PRODUCER
            $CUR_DIR/run/run-input.sh $BS

            for PC in "${COMPUTERS[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                $CUR_DIR/run/run-test-read.sh $TP $PC
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done

$CUR_DIR/result/result-download.sh | $CUR_DIR/result/result-parse.sh > out.`date +%s`.txt
