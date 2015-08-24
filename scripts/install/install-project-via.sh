#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    echo -e $ERR You must specify server $OFF
    exit 1;
fi
SERVER=$1


# COPY
tar -cf project.tar $CUR_DIR/../../src $CUR_DIR/../../pom.xml $CUR_DIR/../../repo
scp project.tar $SERVER:/tmp/
rm project.tar

ssh $SERVER "
    scp /tmp/project.tar $SRV_NIMBUS:/$WRK/project.new.tar
    rm /tmp/project.tar

    # COMPILE
    ssh $SRV_NIMBUS '
        cd $WRK
        if ! cmp project.tar project.new.tar > /dev/null 2> /dev/null
        then
            rm -rf project
            mkdir project
            mv project.new.tar project.tar
            tar -xf project.tar -C project
            cd project
            mvn -q clean package
        fi
    '
"
