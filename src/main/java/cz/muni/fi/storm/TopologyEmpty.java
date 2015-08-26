package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.FilterCounterBolt;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.GlobalCounterBolt;
import cz.muni.fi.storm.bolts.KafkaBolt;
import cz.muni.fi.storm.spouts.KafkaCounterSpout;
import cz.muni.fi.storm.spouts.MyKafkaSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyEmpty {

    public static void main(String[] args) {
        
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: number_of_computers");
        }
        int numberOfComputers = Integer.parseInt(args[0]);
        
        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());
        
        /*
        ZkHosts zkHosts = new ZkHosts("100.64.25.107:2181");
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "dataset-5part", "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme() {
                @Override
                public Fields getOutputFields() {
                    return new Fields("flow");
                }
            });
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        kafkaConfig.forceFromStart = true;
        
        IRichSpout kafkaSpout = new KafkaSpout(kafkaConfig);*/
        IRichSpout kafkaSpout = new MyKafkaSpout();
        IRichBolt kafkaBolt = new KafkaBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, numberOfComputers);
        builder.setBolt("kafkaBolt", kafkaBolt, numberOfComputers)
                .localOrShuffleGrouping("kafkaSpout");

        /*IRichSpout kafkaCounterSpout = new KafkaCounterSpout();
        IRichBolt globalCounterBolt = new GlobalCounterBolt(numberOfComputers);
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt(numberOfComputers);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaCounterSpout", kafkaCounterSpout, numberOfComputers);
        builder.setBolt("globalCounterBolt", globalCounterBolt)
                .globalGrouping("kafkaCounterSpout");
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("kafkaCounterSpout", ServiceCounter.getStreamIdForService());
        */
        try {
            StormSubmitter.submitTopology("TopologyEmpty", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
