#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

# LOG
echo -e $LOG Waiting for finish test $OFF

# wait for one message to signal test done
$KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic $OUTPUT_TOPIC --max-messages 1
