Requirements
============================
* java 1.6 (or higher)
* Kafka machine (input and output, or both in one)
* ssh connection
* filled input Kafka topic

Configuration
============================

Configuration for zookeeper: (changes need reinstall)

        config/zoo.cfg

Configuration for storm: (changes need reinstall)

        config/storm.yaml

Configuration for scripts: (changes can need reinstall)

        scripts/setenv.sh

Configuration for this project: (changes not need reinstall)

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

b) For topology <b>Filter</b>: Read all flows and filter by source IP.
Result is 1 message: count filtered flows.

        scripts/run-test.sh TopologyFilter computers parallelism

c) For topology <b>Counter</b>: Read all flows and count packets for one IP.
Result is 1 message: count packets for filtered IP.

        scripts/run-test.sh TopologyCounter computers parallelism

d) For topology <b>Aggregation</b>: Read all flows and count packets for all IP
(aggregation by source IP). Result is 1 message: count packets for only filtered IP.

        scripts/run-test.sh TopologyAggregation computers parallelism

e) For topology <b>TopN</b>: Read all flows and count packets for all IP (aggregation
by source IP). Result is 1 messages: sorted top N the greatest count packets for IP.

        scripts/run-test.sh TopologyTopN computers parallelism

f) For topology <b>SynScan</b>: Read all flow and count flows for all IP (aggregation
by source IP). Result is 1 message: sorted top N the greatest count flows for IP.

        scripts/run-test.sh TopologySynScan computers parallelism

g) For topology <b>High transfer</b>: Read flows in time window and detect high
transfer in KB. Result is 1 message every time window contains detected high
transfer from source IP and their high destinations.

        scripts/run-test.sh TopologyHighTrans computers parallelism

i) For topology <b>Reflect DoS</b>: Read flows in time window and detect reflect
DoS on defined services and servers. Result is 1 message every time window contains
detected reflect DoS on servers.

        scripts/run-test.sh TopologyReflectDos computers parallelism

Run all tests
----------------------------

Run for each topology: deploy the topology, fill input Kafka topic and wait for one
result in output Kafka topic. Results is written on standard output.
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
