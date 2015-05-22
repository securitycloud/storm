#!/bin/bash

ALLSERVERS[1]=10.16.31.211
ALLSERVERS[2]=10.16.31.212
ALLSERVERS[3]=10.16.31.213
ALLSERVERS[4]=10.16.31.214
ALLSERVERS[5]=10.16.31.215


SRV_SLAVE[1]=${ALLSERVERS[2]}
SRV_SLAVE[2]=${ALLSERVERS[3]}
SRV_SLAVE[3]=${ALLSERVERS[4]}
SRV_SLAVE[4]=${ALLSERVERS[5]}

SRV_NIMBUS=${ALLSERVERS[1]}
SRV_ZK=${ALLSERVERS[1]}


ISTORM=http://mirror.hosting90.cz/apache/storm/apache-storm-0.9.4/apache-storm-0.9.4.zip
IZK=http://mirror.hosting90.cz/apache/zookeeper/stable/zookeeper-3.4.6.tar.gz
STORM=apache-storm-0.9.4
ZK=zookeeper-3.4.6
GIT=https://github.com/nguyenfilip/storm.git
WRK=/root/fnguyen/workdir

CONFDIR=$WRK/configs
LOGDIR=$WRK/logs
