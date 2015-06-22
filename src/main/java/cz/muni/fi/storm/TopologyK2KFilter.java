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

public class TopologyK2KFilter {

    private static final Logger log = Logger.getLogger(TopologyK2KFilter.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-kafka-to-kafka-with-filter");
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: kafka_producer_ip kafka_consumer_ip");
        }

        String kafkaProducerIp = args[0];
        String kafkaConsumerIp = args[1];
        
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
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt(kafkaConsumerIp, kafkaConsumerPort, kafkaConsumerTopic);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaSpout);
        builder.setBolt("filter-bolt", new FilterBolt("dst_port", "80"))
                .fieldsGrouping("kafka-consumer-spout", new Fields("flow"));
        builder.setBolt("kafka-producer-bolt", kafkaProducerBolt)
                .fieldsGrouping("filter-bolt", new Fields("flow"));

        Config config = new Config();
        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

        try {
            StormSubmitter.submitTopology("Topology-kafka-to-kafka-with-filter", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
