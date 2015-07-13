#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    echo -e $LOG Clearing on $i $OFF
    $CUR_DIR/clean-server.sh $i
done

for i in "${KAFKA_SERVERS[@]}"
do
    echo -e $LOG Clearing kafka on $i $OFF
    $CUR_DIR/clean-kafka.sh $i
done
