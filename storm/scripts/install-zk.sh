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
    wget -q $URL_ZK -O zk.tar.gz
    mkdir zk
    tar -xzf zk.tar.gz -C zk --strip 1
"

scp config/zoo.cfg root@$SERVER:$WRK/zk/conf/zoo.cfg
