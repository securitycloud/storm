#!/bin/bash

. scripts/setenv.sh

echo installing zookeeper on $SRV_ZK
scripts/install-zk.sh $SRV_ZK

for i in "${ALL_SERVERS[@]}"
do
    echo installing storm on $i
    scripts/install-storm.sh $i
done

for i in "${KAFKA_SERVERS[@]}"
do
    echo installing storm-kafka on $i
    scripts/install-kafka.sh $i
done

echo installing project
scripts/install-project.sh
