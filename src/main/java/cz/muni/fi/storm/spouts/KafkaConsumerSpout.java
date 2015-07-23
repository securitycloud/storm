package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import cz.muni.fi.storm.tools.readers.KafkaConsumer;
import cz.muni.fi.storm.tools.readers.Reader;

public class KafkaConsumerSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private String broker;
    private int port;
    private String topic;
    private boolean fromBeginning;
    private Reader kafkaConsumer;
    
    public KafkaConsumerSpout(String broker, int port, String topic, boolean fromBeginning) {
        this.broker = broker;
        this.port = port;
        this.topic = topic;
        this.fromBeginning = fromBeginning;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();       
        this.kafkaConsumer = new KafkaConsumer(broker, port, topic,
                fromBeginning, totalTasks, actualTask);
        this.collector = collector;
    }
    
    @Override
    public void nextTuple() {
        String flow = kafkaConsumer.next();
        if (flow != null) {
            this.collector.emit(new Values(flow));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
    
    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
