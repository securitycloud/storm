#!/bin/bash

. setenv.sh

cd ../
mvn assembly:assembly
storm jar target/storm-1.0-SNAPSHOT-jar-with-dependencies.jar cz.muni.fi.storm.Production
