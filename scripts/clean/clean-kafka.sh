#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# RECREATE WORKING DIRECTORY
ssh $SERVER "
    rm -rf $WRK
    mkdir -p $WRK
"
