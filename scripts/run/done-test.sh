#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

START=`date +%s%N`

if [ "$1" == "loop" ]
then
    # LOG
    echo -e $LOG Output from topic $OFF ^c break it and kill topology

    # Wait for one message to signal test done
    ssh $KAFKA_CONSUMER "
        $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic $OUTPUT_TOPIC
    "
else
    # LOG
    echo -e $LOG Waiting for finish test $OFF

    # Wait for one message to signal test done
    ssh $KAFKA_CONSUMER "
        $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic $OUTPUT_TOPIC --max-messages 1
    "
fi
END=`date +%s%N`

(( DIFFERENT = (END - START) / 1000000  ))

echo This run took: $DIFFERENT milli_seconds
