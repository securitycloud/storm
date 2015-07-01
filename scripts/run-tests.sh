#!/bin/bash

. scripts/setenv.sh

TOPOLOGIES[1]=TopologyKafkaCount
TOPOLOGIES[2]=TopologyKafkaKafka
TOPOLOGIES[3]=TapologyKafkaFilter
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

for TP in "${TOPOLOGIES[@]}"
do
    for PC in "${COMPUTERS[@]}"
    do
        for PTN in "${PARTITIONS[@]}"
        do
            for BS in "${BATCH_SIZE[@]}"
            do
                scripts/run-test.sh $TP $PC $PTN $BS
            done
        done
    done
done
