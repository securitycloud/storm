All scripts run in folder <b>storm</b> (where <b>pom.xml</b> exist).

Configuration
============================

Default configuration zookeeper:

        config/zoo.cfg

Default configuration storm:

        config/storm.yaml

Default configuration scripts:

        scripts/setenv.sh


Prepare Storm on cluster
============================

1.) <i>Clean all PCs in cluster:</i> kill all java programs and clean word directory.

        scripts/clean-cluster.sh

2.) <i>Install all PCs in cluster:</i> copy zookeeper, storm and configured them.

        scripts/install-cluster.sh

3.) <i>Start all PCs in cluster:</i> start zookeeper, nimbus, ui and supervisors.

        scripts/start-cluster.sh

Run Storm on cluster
============================

<i>Deploy to cluster:</i> compile, deploy and run project on storm with arguments.

a) For topology kafka-producer to kafka-consumer:
    <b>number_of_computers</b> on which parallel run all this topology

        scripts/deploy-to-cluster.sh cz.muni.fi.storm.TopologyKafkaToKafka <b>number_of_computers</b>

b) For topology kafka-producer to kafka-consumer with filtering:

        scripts/deploy-to-cluster.sh cz.muni.fi.storm.TopologyKafkaToKafkaWithFilter
