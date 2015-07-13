#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# DOWNLOAD AND EXTRACT
ssh root@$SERVER "
    cd $WRK
    wget -q $URL_STORM -O storm.tar.gz
    mkdir storm
    tar -xzf storm.tar.gz -C storm --strip 1
"
# CONFIGURE
scp $CUR_DIR/../../config/storm.yaml root@$SERVER:$WRK/storm/conf/storm.yaml
