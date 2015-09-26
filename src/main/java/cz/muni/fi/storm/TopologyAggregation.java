package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.GlobalAggCounterBolt;
import cz.muni.fi.storm.bolts.AggPacketCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;

public class TopologyAggregation {

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
        IRichBolt aggPacketCounterBolt = new AggPacketCounterBolt();
        IRichBolt globalAggPacketCounterBolt = new GlobalAggCounterBolt(computers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("aggPacketCounterBolt", aggPacketCounterBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalAggPacketCounterBolt", globalAggPacketCounterBolt)
                .globalGrouping("aggPacketCounterBolt")
                .globalGrouping("aggPacketCounterBolt", TupleUtils.getStreamIdForEndOfWindow());

        try {
            StormSubmitter.submitTopology("TopologyAggregation", config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        } catch (InvalidTopologyException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
