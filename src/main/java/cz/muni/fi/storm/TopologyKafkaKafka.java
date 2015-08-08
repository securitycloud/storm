package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.KafkaBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyKafkaKafka {

    public static void main(String[] args) {
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: number_of_computers from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);
        boolean fromBeginning = ("true".equals(args[1])) ? true : false;

        IRichSpout kafkaSpout = new KafkaSpout(fromBeginning, false);
        IRichBolt kafkaBolt = new KafkaBolt();
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, numberOfComputers);
        builder.setBolt("kafkaBolt", kafkaBolt, numberOfComputers)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("kafkaBolt", ServiceCounter.getStreamIdForService());

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
