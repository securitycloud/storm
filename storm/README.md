This following script run in folder <b>storm</b> (where .pom exist) one by one.

Prepare Storm on cluster
============================

1.) Clean all PCs in cluster: kill all java programs and clean word directory.

        scripts/clean-cluster.sh

2.) Install all PCs in cluster: copy zookeeper, storm and configured them.

        scripts/install-cluster.sh

3.) Start all PCs in cluster: start zookeeper, nimbus, ui and supervisors.

        scripts/start-cluster.sh

Run Storm on cluster
============================

Deploy to cluster: Compile, deploy and run project on storm with arguments.

a) For topology kafka-producer to kafka-consumer:

        scripts/deploy-to-cluster.sh cz.muni.fi.storm.TopologyKafkaToKafka 10.16.31.200 10.16.31.201

b) For topology kafka-producer to kafka-consumer with filtering:

        scripts/deploy-to-cluster.sh cz.muni.fi.storm.TopologyKafkaToKafka 10.16.31.200 10.16.31.201
