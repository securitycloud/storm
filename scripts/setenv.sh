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
SRV_SUPERVISOR[5]=${ALL_SERVERS[1]}

SRV_NIMBUS=${ALL_SERVERS[1]}
SRV_ZK=${ALL_SERVERS[1]}

KAFKA_PRODUCER=10.16.31.200
KAFKA_CONSUMER=10.16.31.201

KAFKA_SERVERS[1]=$KAFKA_PRODUCER
KAFKA_SERVERS[2]=localhost

URL_STORM=http://apache.miloslavbrada.cz/storm/apache-storm-0.9.4/apache-storm-0.9.4.tar.gz
URL_ZK=http://apache.miloslavbrada.cz/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
GIT_KAFKA=https://github.com/securitycloud/kafka.git

KAFKA_INSTALL=/root/kafka/kafka_2.11-0.8.2.1
FLOWS_FILE=/root/out
WRK=/root/stormisti/workdir

INPUT_TOPIC=storm-test
OUTUT_TOPIC=storm-test2
SERVICE_TOPIC=storm-service

ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"
