#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

ssh root@$SERVER "
    cd $WRK
    wget -q $URL_STORM -O storm.tar.gz
    mkdir storm
    tar -xzf storm.tar.gz -C storm --strip 1
"

scp config/storm.yaml root@$SERVER:$WRK/storm/conf/storm.yaml
