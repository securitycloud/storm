#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# KILL ALL JAVA PROCESS AND RECREATE WORKING DIRECTORY
ssh $SERVER "
    killall java
    rm -rf $WRK
    mkdir -p $WRK
"
