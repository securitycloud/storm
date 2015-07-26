package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.PacketCounterBolt;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import cz.muni.fi.storm.spouts.KafkaConsumerSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import java.util.logging.Logger;

public class TopologyKafkaCounterKafka {

    private static final Logger log = Logger.getLogger(TopologyKafkaCounterKafka.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-kafka-counter-kafka");
        
        if (args.length < 4) {
            throw new IllegalArgumentException("Missing argument: number_of_computers kafka_producer_ip kafka_consumer_ip from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);

        String kafkaProducerIp = args[1];
        String kafkaConsumerIp = args[2];
        
        boolean fromBeginning = ("true".equals(args[3])) ? true : false;
        
        int kafkaProducerPort = 9092;
        int kafkaConsumerPort = 9092;
        
        String kafkaProducerTopic = "storm-test";
        String kafkaConsumerTopic = "storm-test";

        if (kafkaProducerTopic.equals(kafkaConsumerTopic)
                && kafkaProducerIp.equals(kafkaConsumerIp)) {
            throw new IllegalArgumentException("It creates loop! Please differnet kafkas or topics.");
        }

        KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(kafkaProducerIp, kafkaProducerPort, kafkaProducerTopic, fromBeginning);
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt(kafkaConsumerIp, kafkaConsumerPort, kafkaConsumerTopic);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaConsumerSpout, numberOfComputers);        
        builder.setBolt("packet-counter-bolt", new PacketCounterBolt("62.148.241.49"), numberOfComputers)
                .localOrShuffleGrouping("kafka-consumer-spout");
        builder.setBolt("kafka-producer-bolt", kafkaProducerBolt, numberOfComputers)
                .localOrShuffleGrouping("packet-counter-bolt");

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());

        try {
            StormSubmitter.submitTopology("TopologyKafkaCounterKafka", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
