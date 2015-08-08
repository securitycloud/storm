package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.CounterBolt;
import cz.muni.fi.storm.bolts.KafkaBolt;
import cz.muni.fi.storm.bolts.SlidingWindowBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import java.util.logging.Logger;

public class TopologyK2KWindowCount {

    private static final Logger log = Logger.getLogger(TopologyK2KWindowCount.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-kafka-to-kafka-with-window-count");
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: number_of_computers from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);
        boolean fromBeginning = ("true".equals(args[1])) ? true : false;

        IRichSpout kafkaSpout = new KafkaSpout(fromBeginning, false);
        IRichBolt kafkaBolt = new KafkaBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("sliding-window-bolt", new SlidingWindowBolt(10, 2))
                .fieldsGrouping("kafkaSpout", new Fields("flow"));
        builder.setBolt("counter-bolt", new CounterBolt())
                .fieldsGrouping("sliding-window-bolt", new Fields("flow"))
                .localOrShuffleGrouping("sliding-window-bolt", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("kafkaBolt", kafkaBolt)
                .fieldsGrouping("counter-bolt", new Fields("count"));

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        config.putAll(new TopologyUtil().loadProperties());

        try {
            StormSubmitter.submitTopology("TopologyK2KWindowCount", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
