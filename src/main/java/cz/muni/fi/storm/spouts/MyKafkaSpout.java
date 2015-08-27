package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.tools.SpoutCollector;
import java.util.List;
import java.util.Map;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class MyKafkaSpout extends BaseRichSpout {
    
    private final KafkaSpout kafkaSpout;
    private SpoutOutputCollector collector;
    private SpoutCollector transfer;
    private List<Object> tuple;
    
    public MyKafkaSpout() {
        /*String broker = (String) stormConf.get("kafkaConsumer.broker");
        int port = new Integer(stormConf.get("kafkaConsumer.port").toString());
        String topic = (String) stormConf.get("kafkaConsumer.topic");*/
        
        ZkHosts zkHosts = new ZkHosts("100.64.25.107:2181");
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "dataset-5part", "", "storm");
        kafkaConfig.forceFromStart = true;
        this.kafkaSpout = new KafkaSpout(kafkaConfig);
    }
    
    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.transfer = new SpoutCollector(collector);
        kafkaSpout.open(stormConf, context, transfer);
    }
    
    @Override
    public void nextTuple() {
        kafkaSpout.nextTuple();
        tuple = transfer.getOutput();
        if (tuple != null) {
            this.collector.emit(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
    
    @Override
    public void close() {
        kafkaSpout.close();
    }
}
