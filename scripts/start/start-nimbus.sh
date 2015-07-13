#!/bin/bash

. scripts/setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi

SERVER=$1

# RUN STORM NIMBUS AND UI
ssh root@$SERVER "
    $WRK/storm/bin/storm nimbus  > /dev/null 2>&1  &
    $WRK/storm/bin/storm ui  > /dev/null 2>&1  &
"
