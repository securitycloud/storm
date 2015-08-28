package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.FilterFlowCounterBolt;
import cz.muni.fi.storm.bolts.GlobalCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyFilter {

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: computers parallelism");
        }
        int computers = Integer.parseInt(args[0]);
        int parallelism =Integer.parseInt(args[1]);
        
        Config config = new Config();
        config.setNumWorkers(computers);
        config.putAll(new TopologyUtil().loadProperties());
        
        IRichSpout kafkaSpout = new KafkaSpout(config);
        IRichBolt filterFlowCounterBolt = new FilterFlowCounterBolt();
        IRichBolt globalCounterBolt = new GlobalCounterBolt(computers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("filterFlowCounterBolt", filterFlowCounterBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalCounterBolt", globalCounterBolt)
                .globalGrouping("filterFlowCounterBolt");

        try {
            StormSubmitter.submitTopology("TopologyFilter", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
