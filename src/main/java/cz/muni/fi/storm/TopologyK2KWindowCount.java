package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.CounterBolt;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import cz.muni.fi.storm.bolts.SlidingWindowBolt;
import cz.muni.fi.storm.spouts.KafkaConsumerSpout;
import java.util.logging.Logger;

public class TopologyK2KWindowCount {

    private static final Logger log = Logger.getLogger(TopologyK2KWindowCount.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-kafka-to-kafka-with-window-count");
        
        if (args.length < 4) {
            throw new IllegalArgumentException("Missing argument: number_of_computers kafka_producer_ip kafka_consumer_ip from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);

        String kafkaProducerIp = args[1];
        String kafkaConsumerIp = args[2];
        
        boolean fromBeginning = ("true".equals(args[3])) ? true : false;
        
        int kafkaProducerPort = 9092;
        String kafkaConsumerPort = "9092";
        
        String kafkaProducerTopic = "storm-test";
        String kafkaConsumerTopic = "storm-test";

        if (kafkaProducerTopic.equals(kafkaConsumerTopic)
                && kafkaProducerIp.equals(kafkaConsumerIp)) {
            throw new IllegalArgumentException("It creates loop! Please differnet kafkas or topics.");
        }

        KafkaConsumerSpout kafkaConsumerSpout = new KafkaConsumerSpout(kafkaProducerIp, kafkaProducerPort, kafkaProducerTopic, fromBeginning);
        KafkaProducerBolt kafkaProducerBolt = new KafkaProducerBolt(kafkaConsumerIp, kafkaConsumerPort, kafkaConsumerTopic);
        
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
        config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
        config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);
        config.setDebug(false);

        try {
            StormSubmitter.submitTopology("TopologyK2KWindowCount", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
