#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify 'Message for log' $OFF
    exit 1;
fi
MESSAGE=$1

echo -e $LOG Logging info to service topic: $SERVICE_TOPIC $OFF
ssh root@$KAFKA_CONSUMER "
    echo $MESSAGE |
        $KAFKA_INSTALL/bin/kafka-console-producer.sh --topic $SERVICE_TOPIC --broker-list localhost:9092
"
