package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.readers.KafkaConsumer;
import cz.muni.fi.storm.tools.readers.Reader;

public class KafkaSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private Reader kafkaConsumer;
    private int nullCount = 0;
    
    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();
        String broker = (String) stormConf.get("kafkaConsumer.broker");
        int port = new Integer(stormConf.get("kafkaConsumer.port").toString());
        String topic = (String) stormConf.get("kafkaConsumer.topic");
        
        this.kafkaConsumer = new KafkaConsumer(broker, port, topic, true, totalTasks, actualTask);
        this.collector = collector;
    }
    
    @Override
    public void nextTuple() {
        String flow = kafkaConsumer.next();
        if (flow != null) {
            this.collector.emit(new Values(flow));
            nullCount = 0;
            
        } else {
            nullCount++;
            if (nullCount > 3) {
                TupleUtils.emitEndOfWindow(this.collector);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
        TupleUtils.declareEndOfWindow(declarer);
    }
    
    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
