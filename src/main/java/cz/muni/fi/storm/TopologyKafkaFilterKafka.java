package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.FilterBolt;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import java.util.logging.Logger;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyKafkaFilterKafka {

    private static final Logger log = Logger.getLogger(TopologyKafkaFilterKafka.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-kafka-filter-kafka");
        
        if (args.length < 3) {
            throw new IllegalArgumentException("Missing argument: number_of_computers kafka_producer_ip kafka_consumer_ip");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);

        String kafkaProducerIp = args[1];
        String kafkaConsumerIp = args[2];
        
        String kafkaProducerPort = "2181";
        String kafkaConsumerPort = "9092";
        
        String kafkaProducerTopic = "storm-test";
        String kafkaConsumerTopic = "storm-test";

        if (kafkaProducerTopic.equals(kafkaConsumerTopic)
                && kafkaProducerIp.equals(kafkaConsumerIp)) {
            throw new IllegalArgumentException("It creates loop! Please differnet kafkas or topics.");
        }

        String zookeeperHost = kafkaProducerIp + ":" + kafkaProducerPort;
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, kafkaProducerTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme() {
            @Override
            public Fields getOutputFields() {
                return new Fields("flow");
            }
        });

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt(kafkaConsumerIp, kafkaConsumerPort, kafkaConsumerTopic, true);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaSpout, numberOfComputers);
        builder.setBolt("filter-bolt", new FilterBolt("src_ip_addr", "63.190.148.47"), numberOfComputers)
                .fieldsGrouping("kafka-consumer-spout", new Fields("flow"));
        builder.setBolt("kafka-producer-bolt", kafkaProducerBolt, numberOfComputers)
                .fieldsGrouping("filter-bolt", new Fields("flow"));

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.put("serviceCounter.ip", kafkaConsumerIp);
        config.put("serviceCounter.port", kafkaConsumerPort);
        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        config.setDebug(false);

        try {
            StormSubmitter.submitTopology("Topology-kafka-filter-kafka", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
