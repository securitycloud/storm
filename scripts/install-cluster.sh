#!/bin/bash

. scripts/setenv.sh

echo -e $LOG Installing zookeeper on $SRV_ZK $OFF
scripts/install-zk.sh $SRV_ZK

for i in "${ALL_SERVERS[@]}"
do
    echo -e $LOG Installing storm on $i $OFF
    scripts/install-storm.sh $i
done

for i in "${KAFKA_SERVERS[@]}"
do
    echo -e $LOG Installing storm-kafka on $i $OFF
    scripts/install-kafka.sh $i
done

echo -e $LOG Installing project $OFF
scripts/install-project.sh
