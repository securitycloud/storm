#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ]
then
    echo -e $ERR You must specify Topology $OFF
    exit 1;
fi
TOPOLOGY=$1

if [ -z "$2" ] 
then
    echo -e $ERR You must specify Number of computers $OFF
    exit 2;
fi
COMPUTERS=$2

if [ -z "$3" ] 
then
    echo -e $ERR You must specify Number of parallelism $OFF
    exit 3;
fi
PARALLELISM=$3

STORM_EXE=$WRK/storm/bin/storm
STORM_JAR=$WRK/project/target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar


# LOG
echo -e $LOG Running topology $TOPOLOGY on $COMPUTERS computers, $PARALLELISM parallelism $OFF

# DEPLOY TO NIMBUS
ssh $SRV_NIMBUS "
    $STORM_EXE jar $STORM_JAR cz.muni.fi.storm.$TOPOLOGY $COMPUTERS $PARALLELISM
"
