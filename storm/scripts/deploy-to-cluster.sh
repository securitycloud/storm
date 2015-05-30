#!/bin/bash

. scripts/setenv.sh

# COMPILE
mvn clean package
if [ "$?" -gt 0 ]
then
    exit 1;
fi

# COPY
scp target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar root@$SRV_NIMBUS:/$WRK

# RUN
ssh root@$SRV_NIMBUS "
    $WRK/storm/bin/storm jar $WRK/storm-1.0-SNAPSHOT-jar-with-dependencies.jar $@
"
