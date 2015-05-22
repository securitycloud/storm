#!/bin/bash

. setenv.sh

cp $CONFDIR/storm-nimbus.yaml $WRK/$STORM/conf/storm.yaml

cd $WRK/$STORM
bin/storm nimbus  > /dev/null 2>&1  &
bin/storm ui  > /dev/null 2>&1  &

