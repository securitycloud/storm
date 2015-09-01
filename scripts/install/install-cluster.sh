#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

echo -e $LOG Installing zookeeper on $SRV_ZK $OFF
$CUR_DIR/install-zk.sh $SRV_ZK

for i in "${ALL_SERVERS[@]}"
do
    echo -e $LOG Installing storm on $i $OFF
    $CUR_DIR/install-storm.sh $i
done

echo -e $LOG Installing project $OFF
$CUR_DIR/install-project.sh
