#!/bin/bash

. setenv.sh

if [ -z "$1" ] 
then
 echo "You must specify script"
 exit 1; 
fi

if [ -z "$2" ] 
then
 echo "You must specify server"
 exit 1; 
fi



echo "Running script $1 on server $2"

SCRIPT=$1
SERVER=$2

ssh root@$SERVER "cd $WRK/remote-scripts;./$SCRIPT"
