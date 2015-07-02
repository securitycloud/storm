#!/bin/bash

. scripts/setenv.sh

TOPOLOGIES[1]=TopologyKafkaCounter
TOPOLOGIES[2]=TopologyKafkaKafka
TOPOLOGIES[3]=TopologyKafkaFilter
TOPOLOGIES[4]=TopologyKafkaFilterKafka

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

echo -e $LOG Recreating input topic $SERVICE_TOPIC on $KAFKA_CONSUMER $OFF
scripts/run-topic.sh $SERVICE_TOPIC 1 $KAFKA_CONSUMER

for TP in "${TOPOLOGIES[@]}"
do
    for PC in "${COMPUTERS[@]}"
    do
        for PTN in "${PARTITIONS[@]}"
        do
            for BS in "${BATCH_SIZE[@]}"
            do
                echo -e $LOG Running test $ACT_TEST/$NUM_TESTS: $OFF
                scripts/run-test.sh $TP $PC $PTN $BS
                ACT_TEST=$((ACT_TEST + 1))
            done
        done
    done
done
