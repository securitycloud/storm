#!/bin/bash

. setenv.sh

./run-script.sh install-zk.sh $SRV_ZK
./run-script.sh install-storm.sh $SRV_NIMBUS

for i in "${SRV_SLAVE[@]}"
do
  ./run-script.sh install-storm.sh $i
done


