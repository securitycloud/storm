package cz.muni.fi.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * This is spout which read from kafka topic.
 * Uses kafka spout from Apache. Only get settings and sets kafka spout from Apache.
 */
public class KafkaSpout extends BaseRichSpout {
    
    private final storm.kafka.KafkaSpout kafkaSpout;

    /**
     * New instance need some settings included configuration of storm.
     * 
     * @param config configuration of storm
     */
    public KafkaSpout(Config config) {
        String topic = (String) config.get("kafkaConsumer.topic");
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;
        this.kafkaSpout = new storm.kafka.KafkaSpout(kafkaConfig);
    }

    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        kafkaSpout.open(stormConf, context, collector);
    }

    @Override
    public void nextTuple() {
        kafkaSpout.nextTuple();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
