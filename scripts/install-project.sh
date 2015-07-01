#!/bin/bash

. scripts/setenv.sh

# COPY
tar -cf project.tar src pom.xml
scp project.tar root@$SRV_NIMBUS:/$WRK/project.new.tar
rm project.tar

# COMPILE
ssh root@$SRV_NIMBUS "
    cd $WRK
    if ! cmp project.tar project.new.tar > /dev/null 2> /dev/null
    then
        rm -rf project
        mkdir project
        mv project.new.tar project.tar
        tar -xf project.tar -C project
        cd project
        mvn clean package
    fi
"
