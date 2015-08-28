package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.MoreFlowsKafkaBolt;
import cz.muni.fi.storm.bolts.SrcFlowCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;

public class TopologySynScan{

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
        IRichBolt srcFlowCounterBolt = new SrcFlowCounterBolt("....S.");
        IRichBolt moreFlowsKafkaBolt = new MoreFlowsKafkaBolt(numberOfComputers);
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("srcFlowCounterBolt", srcFlowCounterBolt, numberOfComputers)
                .localOrShuffleGrouping("kafkaSpout")
                .localOrShuffleGrouping("kafkaSpout", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("moreFlowsKafkaBolt", moreFlowsKafkaBolt, numberOfComputers)
                .fieldsGrouping("srcFlowCounterBolt", new Fields("ip"))
                .allGrouping("srcFlowCounterBolt", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("srcFlowCounterBolt", ServiceCounter.getStreamIdForService());

        try {
            StormSubmitter.submitTopology("TopologySynScan", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
