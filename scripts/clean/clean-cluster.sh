#!/bin/bash

. scripts/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    echo -e $LOG Clearing on $i $OFF
    scripts/clean-server.sh $i
done

for i in "${KAFKA_SERVERS[@]}"
do
    echo -e $LOG Clearing kafka on $i $OFF
    scripts/clean-kafka.sh $i
done
