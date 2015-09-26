Requirements
============================
* java 1.6 or greater
* kafka machine
* ssh connection 

Configuration
============================

Configuration for zookeeper:

        config/zoo.cfg

Configuration for storm:

        config/storm.yaml

Configuration for scripts:

        scripts/setenv.sh

Configuration for this project:

        src/main/resources/storm.properties

Install
============================

Install this project on cluster and run necessary programs:

        scripts/install.sh

Run
============================

Run only one test
----------------------------

Recreate input and output kafka topic. Deploy topology to storm, wait for
initialized this. Start filling input kafka topics and wait for storm write
result 1 message to output topic. If has been done, then kill topology.

a) For topology <b>Empty framework</b>: Read all flows/messages from kafka-producer.
Result is 1 message: count read flows/messages.

        scripts/run-test.sh TopologyEmpty computers parallelism

b) For topology <b>Filter</b>: Read all flows and filter by source ip.
Result is 1 message: count filtered flows.

        scripts/run-test.sh TopologyFilter computers parallelism

c) For topology <b>Counter</b>: Read all flows and count packets for one ip.
Result is 1 message: count packets for filtered ip.

        scripts/run-test.sh TopologyCounter computers parallelism

d) For topology <b>Aggregation</b>: Read all flows and count packets for all ip
(aggregation by source ip). Result is 1 message: count packets for only filtered ip.

        scripts/run-test.sh TopologyAggregation computers parallelism

e) For topology <b>TopN</b>: Read all flows and count packets for all ip (aggregation
by source ip). Result is 1 messages: sorted top N the greatest count packets for ip.

        scripts/run-test.sh TopologyTopN computers parallelism

f) For topology <b>SynScan</b>: Read all flow and count flows for all ip (aggregation
by source ip). Result is 1 message: sorted top N the greatest count flows for ip.

        scripts/run-test.sh TopologySynScan computers parallelism

Run all tests
----------------------------

Run for each topology: deploy the topology, fill input kafka topic and wait for one
result in output kafka topic. Results is written on standard output.
For storing to file e.g:

        scripts/run-all-tests.sh > results.out &
        tail -f results.out

Parse results from standard input (or file as argument) and computes minimal, maximal,
average and all values in flows / s. Finally, for each test print computed values
to standard output.

        scripts/run/results-parse.sh results.out

Uninstall
============================

Uninstall this project from cluster and clear working directory:

        scripts/uninstall.sh
