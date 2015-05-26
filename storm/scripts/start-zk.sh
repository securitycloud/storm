#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

# RUN ZOOKEEPER
ssh root@$SERVER "
    $WRK/zk/bin/zkServer.sh start > /dev/null 2>&1 &
"
