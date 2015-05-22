#!/bin/bash

. setenv.sh


cp $CONFDIR/storm-slave.yaml $WRK/$STORM/conf/storm.yaml

cd $WRK/$STORM
bin/storm supervisor  > /dev/null 2>&1  &
