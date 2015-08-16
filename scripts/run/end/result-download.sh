#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

KAFKA_JAR=$WRK/kafka/kafka-storm/target/kafka-storm-1.0-SNAPSHOT-jar-with-dependencies.jar


# DOWNLOAD RESULTS
java -cp $KAFKA_JAR cz.muni.fi.kafka.storm.KafkaConsumer
