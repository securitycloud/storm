#!/bin/bash

. scripts/setenv.sh

for i in "${ALL_SERVERS[@]}"
do
    echo clearing on $i

    # KILL ALL JAVA PROCESS AND RECREATE WORKING DIRECTORY
    ssh root@$i "
        killall java
        rm -rf $WRK
        mkdir -p $WRK
        mkdir -p $LOG_DIR
    "
done
