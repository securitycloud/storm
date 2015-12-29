package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.AggSynFlowCounterBolt;
import cz.muni.fi.storm.bolts.GlobalAggSortCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;

/**
 * Tests top n aggregation of filtered IPs with TCP syn flag.
 * Reads flows from Kafka, filters by TCP syn flag and counts flows
 * for each IP. When done all flows in input Kafka topic, then send
 * top n sorted number of packets for IP to output Kafka topic.
 */
public class TopologySynScan{

    /**
     * Submits topology for this test.
     * Runs on defined number of computers and multiples by defined number of parallelism.
     * It is 1 base stream, that network flows are flowed.
     * It is second stream, that tag end of window are flowed.
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
        IRichBolt aggSynFlowCounterBolt = new AggSynFlowCounterBolt();
        IRichBolt globalAggSortCounterBolt = new GlobalAggSortCounterBolt(computers * parallelism, "synScan");
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("aggSynFlowCounterBolt", aggSynFlowCounterBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalAggSortCounterBolt", globalAggSortCounterBolt)
                .globalGrouping("aggSynFlowCounterBolt")
                .globalGrouping("aggSynFlowCounterBolt", TupleUtils.getStreamIdForEndOfWindow());

        try {
            StormSubmitter.submitTopology("TopologySynScan", config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        } catch (InvalidTopologyException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
