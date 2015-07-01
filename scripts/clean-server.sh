#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

# KILL ALL JAVA PROCESS AND RECREATE WORKING DIRECTORY
ssh root@$SERVER "
    killall java
    rm -rf $WRK
    mkdir -p $WRK
"
