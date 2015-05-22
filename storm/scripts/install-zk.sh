#!/bin/bash

. setenv.sh

cd $WRK
wget -q $IZK 

tar xvf $ZK.tar.gz

cp $CONFDIR/zk-local.cfg $WRK/$ZK/conf/local.cfg
