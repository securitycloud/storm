#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify Batch size $OFF
    exit 1;
fi
BATCH_SIZE=$1

KAFKA_JAR=$WRK/kafka/kafka-storm/target/kafka-storm-1.0-SNAPSHOT-jar-with-dependencies.jar

# LOG
echo -e $LOG Start producing flows on $KAFKA_PRODUCER $OFF

# PRODUCING FLOWS
ssh root@$KAFKA_PRODUCER "
    java -cp $KAFKA_JAR cz.muni.fi.kafka.storm.KafkaProducer $FLOWS_FILE $BATCH_SIZE
"
