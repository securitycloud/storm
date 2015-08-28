package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.FlowCounterBolt;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.GlobalCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyEmpty {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: number_of_computers");
        }
        int numberOfComputers = Integer.parseInt(args[0]);
        
        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());
        int parallelism = new Integer(config.get("parallelism.number").toString());
        
        IRichSpout kafkaSpout = new KafkaSpout(config);
        IRichBolt flowCounterBolt = new FlowCounterBolt();
        IRichBolt globalCounterBolt = new GlobalCounterBolt(numberOfComputers * parallelism);
        //IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt(numberOfComputers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, numberOfComputers * parallelism);
        builder.setBolt("flowCounterBolt", flowCounterBolt, numberOfComputers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalCounterBolt", globalCounterBolt)
                .globalGrouping("flowCounterBolt");
        //builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                //.globalGrouping("flowCounterBolt", ServiceCounter.getStreamIdForService());

        try {
            StormSubmitter.submitTopology("TopologyEmpty", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
