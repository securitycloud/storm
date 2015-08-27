package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.GlobalPacketCounterBolt;
import cz.muni.fi.storm.bolts.PacketCounterBolt;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyAggregation {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: number_of_computers");
        }
        int numberOfComputers = Integer.parseInt(args[0]);        
        
        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());
        int parallelism = new Integer(config.get("parallelism.number").toString());

        String topic = (String) config.get("kafkaConsumer.topic");
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme() {
                @Override
                public Fields getOutputFields() {
                    return new Fields("flow");
                }
            });
        kafkaConfig.forceFromStart = true;
        
        IRichSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        IRichBolt packetCounterBolt = new PacketCounterBolt();
        IRichBolt globalPacketCounterBolt = new GlobalPacketCounterBolt(numberOfComputers * parallelism);
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt(numberOfComputers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, numberOfComputers * parallelism);
        builder.setBolt("packetCounterBolt", packetCounterBolt, numberOfComputers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalPacketCounterBolt", globalPacketCounterBolt)
                .globalGrouping("packetCounterBolt")
                .globalGrouping("packetCounterBolt", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("packetCounterBolt", ServiceCounter.getStreamIdForService());

        try {
            StormSubmitter.submitTopology("TopologyAggregation", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
