package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.Printer;
import java.util.Arrays;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: zookeeper_ip [name_of_topic]");
        }
        String zkIp = args[0];
        String nimbusHost = args[0];
        String zookeeperHost = zkIp + ":2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        String topic;
        if (args.length >= 2) {
            topic = args[1];
        } else {
            topic = "securitycloud-testing-data";
        }

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        /*
         kafkaConfig.scheme = new SchemeAsMultiScheme(new JsonScheme() {
         @Override
         public Fields getOutputFields() {
         return new Fields("events");
         }
         });*/
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flows-reader", kafkaSpout);
        builder.setBolt("printer", new Printer())
                .shuffleGrouping("flows-reader");

        Config config = new Config();
        config.setMaxTaskParallelism(5);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkIp));
        config.put("outputFile", "/home/radozaj/storm.txt");

        try {
            StormSubmitter.submitTopology("Flows-Topology", config, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
