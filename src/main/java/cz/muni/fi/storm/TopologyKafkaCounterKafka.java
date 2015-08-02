package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.GlobalPacketCounterBolt;
import cz.muni.fi.storm.bolts.PacketCounterBolt;
import cz.muni.fi.storm.spouts.KafkaConsumerSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import java.util.logging.Logger;

public class TopologyKafkaCounterKafka {

    private static final Logger log = Logger.getLogger(TopologyKafkaCounterKafka.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: TopologyKafkaCounterKafka");
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: number_of_computers from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);        
        boolean fromBeginning = ("true".equals(args[1])) ? true : false;

        IRichSpout kafkaConsumerSpout = new KafkaConsumerSpout(fromBeginning, true);
        IRichBolt packetCounterBolt = new PacketCounterBolt("62.148.241.49");
        IRichBolt globalPacketCounterBolt = new GlobalPacketCounterBolt(numberOfComputers);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaConsumerSpout, numberOfComputers);        
        builder.setBolt("packet-counter-bolt", packetCounterBolt, numberOfComputers)
                .localOrShuffleGrouping("kafka-consumer-spout");
        builder.setBolt("global-packet-counter-bolt", globalPacketCounterBolt)
                .globalGrouping("packet-counter-bolt");

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
