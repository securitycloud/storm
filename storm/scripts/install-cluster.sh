#!/bin/bash

. scripts/setenv.sh

echo installing zookeeper on $SRV_ZK
scripts/install-zk.sh $SRV_ZK

for i in "${ALL_SERVERS[@]}"
do
    echo installing storm on $i
    scripts/install-storm.sh $i
done


