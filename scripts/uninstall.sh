#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    echo -e $LOG Clearing on $i $OFF
    $CUR_DIR/uninstall/clean-server.sh $i
done
