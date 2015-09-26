#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    mkdir -p $WRK
    echo -e $LOG Installing storm on $i $OFF
    $CUR_DIR/install/install-storm.sh $i
done

echo -e $LOG Installing zookeeper on $SRV_ZK $OFF
$CUR_DIR/install/install-zk.sh $SRV_ZK

echo -e $LOG Installing project $OFF
$CUR_DIR/install/install-project.sh

echo -e $LOG Starting zookeeper on $SRV_ZK $OFF
$CUR_DIR/install/start-zk.sh $SRV_ZK

sleep 5
echo -e $LOG Starting nimbus and ui on $SRV_NIMBUS $OFF
$CUR_DIR/install/start-nimbus.sh $SRV_NIMBUS

for i in "${SRV_SUPERVISOR[@]}"
do
    echo -e $LOG Starting supervisor on $i $OFF
    $CUR_DIR/install/start-supervisor.sh $i
done