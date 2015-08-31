#!/bin/bash

CUR_DIR=`dirname $0`
. $CUR_DIR/../../setenv.sh

if [ -z "$1" ] 
then
    SOURCE=/dev/stdin
else
    SOURCE=$1
fi

declare -a TEST_COUNT=( $(for i in {1..100}; do echo 0; done) )
declare -a TEST_SUM=( $(for i in {1..100}; do echo 0; done) )
declare -a TEST_MIN=( $(for i in {1..100}; do echo 999999999; done) )
declare -a TEST_MAX=( $(for i in {1..100}; do echo 0; done) )

while read LINE
do
    # NAME OF TEST
    LABEL=`echo $LINE | sed 's/^ Running topology \(.*\)$/\1/'`
    if [[ $LABEL =~ $Topology.* ]]
    then
        i=1
        for NAME in "${TEST_NAME[@]}"
        do
            if [ "$NAME" = "$LABEL" ] 
            then
                break
            fi
            i=$((i + 1))
        done
        TEST_NAME[i]="$LABEL"
        TEST_INDEX=$i
    fi
    
    # RESULT OF TEST
    RESULT=`echo $LINE | sed 's/^This run took: \([0-9]*\) milli_seconds$/\1/'`
    if [[ $RESULT =~ ^[0-9]+$ ]]
    then
        (( RESULT = 1000000 / RESULT ))
        (( TEST_COUNT[TEST_INDEX]++ ))
        (( TEST_SUM[TEST_INDEX] += RESULT ))
        (( TEST_MAX[TEST_INDEX] = RESULT > TEST_MAX[TEST_INDEX] ? RESULT : TEST_MAX[TEST_INDEX] ))
        (( TEST_MIN[TEST_INDEX] = RESULT < TEST_MIN[TEST_INDEX] ? RESULT : TEST_MIN[TEST_INDEX] ))
        TEST_ALL[$TEST_INDEX]=${TEST_ALL[$TEST_INDEX]}, $RESULT
    fi    

done < $SOURCE

# WRITE RESULTS
for i in `seq 1 ${#TEST_NAME[@]}`
do
    if [ "${TEST_COUNT[$i]}" -gt 0 ]
    then
        echo ${TEST_NAME[$i]}
        (( AVG = TEST_SUM[i] / TEST_COUNT[i] ))
        echo "Results for ${TEST_COUNT[$i]} tests:"
        echo "  all = ${TEST_ALL[$i]}"
        echo "  min = ${TEST_MIN[$i]} flows / s"
        echo "  avg = $AVG flows / s"
        echo "  max = ${TEST_MAX[$i]} flows / s"
        echo
    fi
done
