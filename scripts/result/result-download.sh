#!/bin/bash

. scripts/setenv.sh

# DOWNLOAD RESULTS
ssh root@$KAFKA_CONSUMER "
    $KAFKA_INSTALL/bin/kafka-console-consumer.sh --topic $SERVICE_TOPIC --zookeeper localhost:2181 --from-beginning > /tmp/storm-results
" &
PID=$!
sleep 3
kill $PID

# COPY TO LOCAL
scp root@$KAFKA_CONSUMER:/tmp/storm-results /tmp/storm-results

# TO STANDARD OUTPUT
cat /tmp/storm-results