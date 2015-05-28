package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.KafkaProducerBolt;
import cz.muni.fi.storm.bolts.Printer;
import java.util.Arrays;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyMain{

    public static void main(String[] args) {
      /*  if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: zookeeper_ip [name_of_topic]");
        }*/
        
        /*
        First argument = zookeeper & nibmus host Ip
        Second argument = topic name, String
        Third argument = kafka consumer Ip address;
        */
        
        String zkIp = "10.16.31.211";
        String nimbusHost = "10.16.31.211";
        String kafkaConsumerIp="10.16.31.200";
        String zookeeperHost = zkIp + ":2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);
        
        if(args.length >=3) {
           kafkaConsumerIp=args[2];
        }else {
            kafkaConsumerIp ="localhost";
        }

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
        /*builder.setBolt("printer", new Printer())
                .shuffleGrouping("flows-reader");*/
        builder.setBolt("printer", new KafkaProducerBolt(kafkaConsumerIp))
                .shuffleGrouping("flows-reader");
        
        Config config = new Config();
        config.setMaxTaskParallelism(5);
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkIp));
        //config.put("outputFile", "/root/stormisti/result.txt");

        try {
            StormSubmitter.submitTopology("Flows-Topology", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
