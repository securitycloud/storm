package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import cz.muni.fi.storm.spouts.KafkaConsumerSpout;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyKafkaKafka {

    public static void main(String[] args) {
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: number_of_computers from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);
        boolean fromBeginning = ("true".equals(args[1])) ? true : false;

        KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(fromBeginning, false);
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaConsumerSpout, numberOfComputers);
        builder.setBolt("kafka-producer-bolt", kafkaProducerBolt, numberOfComputers)
                .localOrShuffleGrouping("kafka-consumer-spout");

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());
        
        try {
            StormSubmitter.submitTopology("TopologyKafkaKafka", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
