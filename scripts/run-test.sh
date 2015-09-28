#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

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

# If 4. parameter in not exist, try update project
if [ -n "$4" ]
then
    $CUR_DIR/run/install-project.sh
fi
$CUR_DIR/run/run-topology.sh $TOPOLOGY $COMPUTERS $PARALLELISM
$CUR_DIR/run/done-test.sh
$CUR_DIR/run/kill-topology.sh $TOPOLOGY
