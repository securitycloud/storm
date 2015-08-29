package cz.muni.fi.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.tools.SpoutCollector;
import java.util.Map;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaSpout2 extends BaseRichSpout {
    
    private final storm.kafka.KafkaSpout kafkaSpout;
    private SpoutOutputCollector collector;
    private SpoutCollector fakeCollector;

    public KafkaSpout2(Config config) {
        String topic = (String) config.get("kafkaConsumer.topic");
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        ZkHosts zkHosts = new ZkHosts(zookeeper);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme() {
            @Override
            public Fields getOutputFields() {
                return new Fields("flow");
            }
        });
        kafkaConfig.forceFromStart = true;
        this.kafkaSpout = new storm.kafka.KafkaSpout(kafkaConfig);
    }

    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.fakeCollector = new SpoutCollector(collector);
        kafkaSpout.open(stormConf, context, fakeCollector);
    }

    @Override
    public void nextTuple() {
        fakeCollector.cleanOutput();
        kafkaSpout.nextTuple();
        if (fakeCollector.getOutput() != null) {
            collector.emit(fakeCollector.getOutput());
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
