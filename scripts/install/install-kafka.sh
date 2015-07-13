#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# DOWNLOAD AND COMPILE
ssh root@$SERVER "
    cd $WRK
    git clone $GIT_KAFKA
    cd kafka/kafka-storm
    mvn -q clean package
"
