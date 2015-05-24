#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    MAIN_CLASS=cz.muni.fi.storm.TopologyMain
else
    MAIN_CLASS=$1
fi

mvn clean package
if [ "$?" -eq 1 ]
then
    exit 1;
fi

scp target/storm-1.0-SNAPSHOT.jar root@$SRV_NIMBUS:/$WRK

ssh root@$SRV_NIMBUS "
    $WRK/storm/bin/storm jar $WRK/storm-1.0-SNAPSHOT.jar $MAIN_CLASS
"
