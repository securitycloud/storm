#!/bin/bash

. setenv.sh

for i in "${ALLSERVERS[@]}"
do
  ssh root@$i "killall java"
  ssh root@$i "rm -rf *yaml; rm -rf *cfg; rm -rf $WRK; mkdir -p $WRK; mkdir -p $LOGDIR; mkdir $WRK/remote-scripts; mkdir $CONFDIR"
  scp *sh root@$i:$WRK/remote-scripts
  scp ../config/* root@$i:$CONFDIR
done
