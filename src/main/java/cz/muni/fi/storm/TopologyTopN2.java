package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.GlobalAggSortCounterBolt;
import cz.muni.fi.storm.bolts.AggSortPacketCounterBolt;
import cz.muni.fi.storm.bolts.PacketSenderBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;

public class TopologyTopN2 {

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
        IRichBolt packetSenderBolt = new PacketSenderBolt();
        IRichBolt aggSortPacketCounterBolt = new AggSortPacketCounterBolt(computers * parallelism);
        IRichBolt globalAggSortCounterBolt = new GlobalAggSortCounterBolt(computers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("packetSenderBolt", packetSenderBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("aggSortPacketCounterBolt", aggSortPacketCounterBolt, computers * parallelism)
                .fieldsGrouping("packetSenderBolt", new Fields("ip"))
                .allGrouping("packetSenderBolt", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("globalAggSortCounterBolt", globalAggSortCounterBolt)
                .globalGrouping("aggSortPacketCounterBolt")
                .globalGrouping("aggSortPacketCounterBolt", TupleUtils.getStreamIdForEndOfWindow());

        try {
            StormSubmitter.submitTopology("TopologyTopN2", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
