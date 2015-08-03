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

a) <i>Read tests:</i> It prepares storm on cluster, fills kafka-producer by input file
and runs all topologies one by one. It downloads and parses results and saves to out.date.txt

    scripts/all-tests-read.sh

b) <i>ReadWrite tests:</i> It prepares storm on cluster and runs all topologies one by one
with filling kafka-producer by input file. It downloads and parses results and saves to
out.date.txt

    scripts/all-tests-readwrite.sh

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

<i>Run test on cluster:</i> <b>ReadWrite test</b> open testing kafka topics, start topology for actual test
and begin sent testing data to topology. If readWrite test finish producing data, then it kill topology.
<b>Read test</b> open testing kafka topic only on kafka-consumer (topic on kafka-producer must exist and filled)
and start topology for actual test. Read test wait 7 minutes and kill topology.

All topologies are sent delay between every millionth flow in ms to kafka-consumer topic <b>storm-service</b>.
Default kafka topic is <b>storm-test</b>.

a) For topology KafkaConsumer -> KafkaProducer
        (Scheme: KafkaConsumerSpout -> KafkaProducerBolt):

        scripts/run/run-test-read.sh TopologyKafkaKafka number_of_computers
        scripts/run/run-test-readwrite.sh TopologyKafkaKafka number_of_computers partitions batch_size

b) For topology KafkaConsumer -> Filter -> KafkaProducer
        (Sckeme: KafkaConsumerSpout -> FilterBolt -> KafkaProducerBolt):

        scripts/run/run-test-read.sh TopologyKafkaFilterKafka number_of_computers
        scripts/run/run-test-readwrite.sh TopologyKafkaFilterKafka number_of_computers partitions batch_size

c) For topology KafkaConsumer -> Filter -> PacketCounter -> GlobalPacketCounter -> KafkaProducer
        (Scheme: KafkaConsumerSpout -> PacketCounterBolt -> GlobalPacketCounterBolt):

        scripts/run/run-test-read.sh TopologyKafkaCounterKafka number_of_computers
        scripts/run/run-test-readwrite.sh TopologyKafkaCounterKafka number_of_computers partitions batch_size

d) For topology KafkaConsumer -> PacketCounter -> GlobalPacketCounter -> KafkaProducer
        (Scheme: KafkaConsumerSpout -> PacketCounterBolt -> GlobalPacketCounterBolt):

        scripts/run/run-test-read.sh TopologyKafkaAggregationKafka number_of_computers
        scripts/run/run-test-readwrite.sh TopologyKafkaAggregationKafka number_of_computers partitions batch_size

z) For topology FileReader -> FileWriter
        (Scheme: FileReaderSpout -> FileWriterBolt):

        scripts/run/run-test-read.sh TopologyFileFile number_of_computers
        scripts/run/run-test-readwrite.sh TopologyFileFile number_of_computers partitions batch_size

Results of testing
----------------------------

1.) <i>Download results from kafka-consumer</i> and print to standard output:

        scripts/results/results-download.sh

2.) <i>Parse results:</i> It parses results from standard input (or file as argument) and computes
minimal, maximal and average values in flows / s. On standard output it sends all downloaded results
and for each test print computed values.

        scripts/results/results-parse.sh