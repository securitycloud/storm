#!/bin/bash

. setenv.sh

cd $WRK/$ZK
java -cp zookeeper-3.4.6.jar:lib/*:conf org.apache.zookeeper.server.quorum.QuorumPeerMain conf/local.cfg > /dev/null 2>&1 &
