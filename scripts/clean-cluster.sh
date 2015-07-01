#!/bin/bash

. scripts/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    echo clearing on $i
    scripts/clean-server.sh $i
done

for i in "${KAFKA_SERVERS[@]}"
do
    echo clearing kafka on $i
    scripts/clean-kafka.sh $i
done
