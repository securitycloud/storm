#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# DOWNLOAD AND EXTRACT
ssh root@$SERVER "
    cd $WRK
    wget -q $URL_ZK -O zk.tar.gz
    mkdir zk
    tar -xzf zk.tar.gz -C zk --strip 1
"

#CONFIGURE
scp config/zoo.cfg root@$SERVER:$WRK/zk/conf/zoo.cfg
