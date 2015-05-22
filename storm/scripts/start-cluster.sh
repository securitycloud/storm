#!/bin/bash

. setenv.sh

./run-script.sh start-zk.sh $SRV_ZK
sleep 5
./run-script.sh start-nimbus.sh $SRV_NIMBUS


for i in "${SRV_SLAVE[@]}"
do
  ./run-script.sh start-slave.sh $i
done


