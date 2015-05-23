#!/bin/bash

. scripts/setenv.sh

echo starting zookeeper on $SRV_ZK
scripts/start-zk.sh $SRV_ZK

sleep 5
echo starting nimbus and ui on $SRV_NIMBUS
scripts/start-nimbus.sh $SRV_NIMBUS


for i in "${SRV_SUPERVISOR[@]}"
do
    echo starting supervisor on $i
    scripts/start-supervisor.sh $i
done


