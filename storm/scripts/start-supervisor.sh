#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

# RUN STORM SUPERVISOR
ssh root@$SERVER "
    $WRK/storm/bin/storm supervisor  > /dev/null 2>&1  &
"
