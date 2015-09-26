package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.FilterPacketCounterBolt;
import cz.muni.fi.storm.bolts.GlobalCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;

/**
 * Tests counted packets.
 * Reads flows from kafka, counts their packets for chosen ip. When done all flows
 * in input kafka topic, then send number of packets for chosen ip to output kafka topic.
 */
public class TopologyCounter {

    /**
     * Submits topology for this test.
     * Runs on defined number of computers and multiples by defined number of parallelism.
     * It is only 1 base stream, which network flows are flowed.
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
        IRichBolt filterPacketCounterBolt = new FilterPacketCounterBolt();
        IRichBolt globalCounterBolt = new GlobalCounterBolt(computers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("filterPacketCounterBolt", filterPacketCounterBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalCounterBolt", globalCounterBolt)
                .globalGrouping("filterPacketCounterBolt");

        try {
            StormSubmitter.submitTopology("TopologyCounter", config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        } catch (InvalidTopologyException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
