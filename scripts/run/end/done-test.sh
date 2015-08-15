#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

# LOG
echo -e $LOG Waiting for finish test $END

# UNTIL TEST HAS BEEN DONE
while [ true ]
do
    DONE=true

    $CUR_DIR/result-download.sh > /tmp/done-test
    while read LINE
    do
        # LINE IS NAME OF TEST
        if [[ ${LINE::1} =~ "T" ]]
        then
            DONE=false
        fi

        # LINE IS RESULT OF TEST
        if [[ ${LINE::1} =~ [0-9] ]]
        then
            DONE=true
        fi
    done < /tmp/done-test

    if [ "$DONE" = "true" ]; then break; fi
    sleep 60
done
