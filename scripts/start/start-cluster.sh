#!/bin/bash

. scripts/setenv.sh

echo -e $LOG Starting zookeeper on $SRV_ZK $OFF
scripts/start-zk.sh $SRV_ZK

sleep 5
echo -e $LOG Starting nimbus and ui on $SRV_NIMBUS $OFF
scripts/start-nimbus.sh $SRV_NIMBUS


for i in "${SRV_SUPERVISOR[@]}"
do
    echo -e $LOG Starting supervisor on $i $OFF
    scripts/start-supervisor.sh $i
done


