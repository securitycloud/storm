#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../setenv.sh

if [ -z "$1" ] 
then
    SOURCE=/dev/stdin
else
    SOURCE=$1
fi


FIRST=true

while read LINE
do
    if [[ ${LINE::1} =~ [a-zA-Z] ]]
    then
        if [ "$FIRST" = "true" ]
        then
            FIRST=false
        else
            if [ $COUNT -gt 0 ]
            then
                RESULT=$(($COMPUTERS * $COUNT * 1000000000 / $SUM))
                echo RESULT = $RESULT flows/s
            fi
        fi
            
        COMPUTERS=0
        COUNT=0
        SUM=0
    fi

    echo $LINE

    if [[ $LINE =~ ^[0-9]+$ ]]
    then
        if [ $LINE -gt 1000000000 ]
        then
            COMPUTERS=$((COMPUTERS + 1))
        else
            SUM=$((SUM + LINE))
            COUNT=$((COUNT + 1))
        fi
    fi
done < $SOURCE
