#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

# LOG
echo -e $LOG Waiting for finish test $OFF

START=`date +%s%N`

# Wait for one message to signal test done
ssh $KAFKA_CONSUMER "
    $KAFKA_INSTALL/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic $OUTPUT_TOPIC --max-messages 1
"

END=`date +%s%N`

(( DIFFERENT = (END - START) / 1000000  ))

echo This run took: $DIFFERENT milli_seconds
