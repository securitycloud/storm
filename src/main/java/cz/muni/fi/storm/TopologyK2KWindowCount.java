package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.CounterBolt;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import cz.muni.fi.storm.bolts.SlidingWindowBolt;
import cz.muni.fi.storm.spouts.KafkaConsumerSpout;
import cz.muni.fi.storm.tools.TopologyUtil;
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

        KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(fromBeginning, false);
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-consumer-spout", kafkaConsumerSpout);
        builder.setBolt("sliding-window-bolt", new SlidingWindowBolt(10, 2))
                .fieldsGrouping("kafka-consumer-spout", new Fields("flow"));
        builder.setBolt("counter-bolt", new CounterBolt())
                .fieldsGrouping("sliding-window-bolt", new Fields("flow"));
        builder.setBolt("kafka-producer-bolt", kafkaProducerBolt)
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
