#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# RUN STORM SUPERVISOR
ssh root@$SERVER "
    $WRK/storm/bin/storm supervisor  > /dev/null 2>&1  &
"
