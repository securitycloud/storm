#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

if [ -z "$1" ]
then
    echo -e $ERR You must specify Topology $OFF
    exit 1;
fi
TOPOLOGY=$1


STORM_EXE=$WRK/storm/bin/storm

# LOG
echo -e $LOG Killing topology $TOPOLOGY $OFF

# KILL TOPOLOGY
ssh $SRV_NIMBUS "
    $STORM_EXE kill $TOPOLOGY 1
    sleep 1
    while $STORM_EXE kill $TOPOLOGY 1 2> /dev/null
    do
        sleep 5
    done
"
