Configuration
============================

Default configuration for zookeeper:

        config/zoo.cfg

Default configuration for storm:

        config/storm.yaml

Default configuration for scripts:

        scripts/setenv.sh

Default configuration for this project:

        src/main/resources/storm.properties

One script for testing
============================

It prepares storm on cluster, it excepts filled input kafka topic for computers >= partitions 
and runs all topologies one by one. It downloads and parses results and saves to out.date.txt

    scripts/all-tests-read.sh

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

<i>Run test on cluster:</i> It opens testing output kafka topic only on kafka-consumer
(topic on kafka-producer must exist and filled) and it starts topology for actual test.
If test has been done, then it kill topology.

All topologies are implemented count window and they are sent working time in ms to
kafka-consumer service topic.

a) For topology <b>Empty framework</b>: Read all flows/messages from kafka-producer
and write down count flows/messages.

        scripts/run/run-test.sh TopologyEmpty number_of_computers

b) For topology <b>Filter</b>: Read all flows/messages and filter by source ip. Finally,
write down count filtered flows/messages.

        scripts/run/run-test.sh TopologyFilter number_of_computers

c) For topology KafkaConsumer -> Filter -> PacketCounter -> GlobalPacketCounter -> KafkaProducer:

        scripts/run/run-test.sh TopologyKafkaCounterKafka number_of_computers

d) For topology KafkaConsumer -> DstPacketCounter -> GlobalPacketCounter -> KafkaProducer:

        scripts/run/run-test.sh TopologyKafkaAggregationKafka number_of_computers

e) For topology KafkaConsumer -> DstPacketCounter -> GlobalSortPackets -> KafkaProducer:

        scripts/run/run-test.sh TopologyKafkaTopNKafka number_of_computers

f) For topology KafkaConsumer -> SrcFlowCounter -> MoreFlows -> KafkaProducer:

        scripts/run/run-test.sh TopologyKafkaTcpSynKafka number_of_computers

Results of testing
----------------------------

1.) <i>Download results from kafka-consumer</i> and print to standard output:

        scripts/results/results-download.sh

2.) <i>Parse results:</i> It parses results from standard input (or file as argument) and computes
minimal, maximal and average values in flows / s. Finally, for each test print computed values to
standard output.

        scripts/results/results-parse.sh