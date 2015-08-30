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

One script for testing
============================

Prepare storm on cluster. For each topology do: deploy the topology, fill input kafka
topic and wait for one result in output kafka topic.

    scripts/all-tests.sh

(Run this script with any argument mean: without prepare storm on cluster. Only try redeploy
this project and run tests for each topology.)

Manual scripts for custom testing
============================

Prepare Storm on cluster
----------------------------

1.) <i>Clean all PCs in cluster:</i> kill all java programs on work PCs.
Clean work directory on all PCs in cluster.

        scripts/clean/clean-cluster.sh

2.) <i>Install all PCs in cluster:</i> copy zookeeper, storm and configured them.
Download and compile kafka-storm on kafka PCs. Download and compile project to nimbus.

        scripts/install/install-cluster.sh

3.) <i>Start all PCs in cluster:</i> start zookeeper, nimbus, ui and supervisors.

        scripts/start/start-cluster.sh

Run Storm on cluster
----------------------------

<i>Run test on cluster:</i> Recreate input and output kafka topic. Deploy topology
to storm, wait for initialized this. Start filling input kafka topics and wait for
storm write result 1 message to output topic. If has been done, then kill topology.

a) For topology <b>Empty framework</b>: Read all flows/messages from kafka-producer.
Result is 1 message: count read flows/messages.

        scripts/run/run-test.sh TopologyEmpty computers parallelism

b) For topology <b>Filter</b>: Read all flows and filter by source ip.
Result is 1 message: count filtered flows.

        scripts/run/run-test.sh TopologyFilter computers parallelism

c) For topology <b>Counter</b>: Read all flows and count packets for one ip.
Result is 1 message: count packets for filtered ip.

        scripts/run/run-test.sh TopologyCounter computers parallelism

d) For topology <b>Aggregation</b>: Read all flows and count packets for all ip
(aggregation by source ip). Result is 1 message: count packets for only filtered ip.

        scripts/run/run-test.sh TopologyAggregation computers parallelism

e) For topology <b>TopN</b>: Read all flows and count packets for all ip (aggregation
by source ip). Result is 1 messages: sorted top N the greatest count packets for ip.

        scripts/run/run-test.sh TopologyTopN computers parallelism

f) For topology <b>SynScan</b>: Read all flow and count flows for all ip (aggregation
by source ip). Result is 1 message: sorted top N the greatest count flows for ip.

        scripts/run/run-test.sh TopologySynScan computers parallelism

Results of testing
----------------------------

1.) <i>Download results</i>: results is written on standard output. For storing to file e.g:

        scripts/all-tests.sh > results.out &
        tail -f results.out

2.) <i>Parse results:</i> Parse results from standard input (or file as argument) and computes
minimal, maximal and average values in flows / s. Finally, for each test print computed values
to standard output.

        scripts/results/results-parse.sh results.out