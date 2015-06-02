#!/bin/bash

ALL_SERVERS[1]=10.16.31.211
ALL_SERVERS[2]=10.16.31.212
ALL_SERVERS[3]=10.16.31.213
ALL_SERVERS[4]=10.16.31.214
ALL_SERVERS[5]=10.16.31.215

SRV_SUPERVISOR[1]=${ALL_SERVERS[2]}
SRV_SUPERVISOR[2]=${ALL_SERVERS[3]}
SRV_SUPERVISOR[3]=${ALL_SERVERS[4]}
SRV_SUPERVISOR[4]=${ALL_SERVERS[5]}

SRV_NIMBUS=${ALL_SERVERS[1]}
SRV_ZK=${ALL_SERVERS[1]}

URL_STORM=http://mirror.hosting90.cz/apache/storm/apache-storm-0.9.4/apache-storm-0.9.4.tar.gz
URL_ZK=http://mirror.hosting90.cz/apache/zookeeper/stable/zookeeper-3.4.6.tar.gz
WRK=/root/stormisti/workdir

LOG_DIR=$WRK/logs
