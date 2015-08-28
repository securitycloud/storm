package cz.muni.fi.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaSpout2 extends BaseRichSpout {
    
    private KafkaSpout kafkaSpout;

    public KafkaSpout2(Config config) {
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        String broker = (String) config.get("kafkaConsumer.broker");
        ZkHosts zkHosts = new ZkHosts(broker);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, zookeeper, "", "storm");
        kafkaConfig.forceFromStart = true;
        this.kafkaSpout = new KafkaSpout(kafkaConfig);
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
