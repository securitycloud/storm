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

/**
 * Tests aggregation of ips.
 * Reads flows from kafka, counts their packets for each ip. Stores all packets
 * for each ip, but when done all flows in input kafka topic, then send number
 * of packets for only chosen ip to output kafka topic.
 */
public class TopologyAggregation {

    /**
     * Submits topology for this test.
     * Runs on defined number of computers and multiples by defined number of parallelism.
     * It is 1 base stream, which network flows are flowed.
     * It is second stream, which tag end of window are flowed.
     * 
     * @param args number of computers and number of parallelism.
     */
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
