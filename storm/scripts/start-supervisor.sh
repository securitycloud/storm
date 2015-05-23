#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo "You must specify server"
    exit 1;
fi

SERVER=$1

ssh root@$SERVER "
    $WRK/storm/bin/storm supervisor  > /dev/null 2>&1  &
"
