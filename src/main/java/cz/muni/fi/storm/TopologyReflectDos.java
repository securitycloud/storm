package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.GlobalReflectDosBolt;
import cz.muni.fi.storm.bolts.LocalReflectDosBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;

/**
 * Tests detection of Reflect DoS.
 * Reads flows from Kafka, in batch (in time window) detects reflect DoS on defined services.
 * Detected anomaly are emitted to output Kafka topic.
 */
public class TopologyReflectDos {

    /**
     * Submits topology for this test.
     * Runs on defined number of computers and multiples by defined number of parallelism.
     * There is 1 base stream, that network flows are flowed.
     * There is second stream, that tag end of window are flowed.
     * 
     * @param args number of computers and number of parallelism.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: computers parallelism");
        }
        int computers = Integer.parseInt(args[0]);
        int parallelism = Integer.parseInt(args[1]);
        
        Config config = new Config();
        config.setNumWorkers(computers);
        config.putAll(new TopologyUtil().loadProperties());
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);

        IRichSpout kafkaSpout = new KafkaSpout(config);
        IRichBolt localReflectDosBolt = new LocalReflectDosBolt();
        IRichBolt globalReflectDosBolt = new GlobalReflectDosBolt(computers * parallelism);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, computers * parallelism);
        builder.setBolt("localReflectDosBolt", localReflectDosBolt, computers * parallelism)
                .localOrShuffleGrouping("kafkaSpout");
        builder.setBolt("globalReflectDosBolt", globalReflectDosBolt)
                .globalGrouping("localReflectDosBolt")
                .globalGrouping("localReflectDosBolt", TupleUtils.getStreamIdForEndOfWindow());
        
        try {
            StormSubmitter.submitTopology("TopologyReflectDos", config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        } catch (InvalidTopologyException e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
