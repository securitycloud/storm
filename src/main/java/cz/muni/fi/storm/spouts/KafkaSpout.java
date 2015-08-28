package cz.muni.fi.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaSpout extends BaseRichSpout {
    
    private final storm.kafka.KafkaSpout kafkaSpout;

    public KafkaSpout(Config config) {
        String topic = (String) config.get("kafkaConsumer.topic");
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "storm");
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
