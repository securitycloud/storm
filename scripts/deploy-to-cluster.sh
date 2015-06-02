#!/bin/bash

. scripts/setenv.sh

# COPY
tar -cf project.tar src pom.xml
scp project.tar root@$SRV_NIMBUS:/$WRK
rm project.tar

# COMPILE & RUN
ssh root@$SRV_NIMBUS "
    cd $WRK
    rm -rf project
    mkdir project
    tar -xf project.tar -C project
    cd project
    mvn clean package
    if [ "$?" -gt 0 ]
    then
        exit 1;
    fi
    $WRK/storm/bin/storm jar $WRK/project/target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar $@
"
