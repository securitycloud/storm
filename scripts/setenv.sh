#!/bin/bash

ALL_SERVERS[1]=100.64.25.101
ALL_SERVERS[2]=100.64.25.102
ALL_SERVERS[3]=100.64.25.103
ALL_SERVERS[4]=100.64.25.104
ALL_SERVERS[5]=100.64.25.105

SRV_SUPERVISOR[1]=${ALL_SERVERS[1]}
SRV_SUPERVISOR[2]=${ALL_SERVERS[3]}
SRV_SUPERVISOR[3]=${ALL_SERVERS[4]}
SRV_SUPERVISOR[4]=${ALL_SERVERS[5]}
SRV_SUPERVISOR[5]=${ALL_SERVERS[2]}

SRV_NIMBUS=${ALL_SERVERS[2]}
SRV_ZK=${ALL_SERVERS[2]}

KAFKA_PRODUCER=100.64.25.107
KAFKA_CONSUMER=100.64.25.107

KAFKA_SERVERS[1]=$KAFKA_PRODUCER
KAFKA_SERVERS[2]=localhost

URL_STORM=http://apache.miloslavbrada.cz/storm/apache-storm-0.9.4/apache-storm-0.9.4.tar.gz
URL_ZK=http://apache.miloslavbrada.cz/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
GIT_KAFKA=https://github.com/securitycloud/kafka.git

KAFKA_INSTALL=/home/securitycloud/kafka/kafka_2.9.2-0.8.2.1
FLOWS_FILE=/home/securitycloud/out_small
WRK=/home/securitycloud/stormisti/workdir

INPUT_TOPIC=dataset-5part
OUTUT_TOPIC=storm-test
SERVICE_TOPIC=storm-service

ERR="\033[1;31m"
OK="\033[1;32m"
LOG="\033[1;34m"
OFF="\033[0m"
