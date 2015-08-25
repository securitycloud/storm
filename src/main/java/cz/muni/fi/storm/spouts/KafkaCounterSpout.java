package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;
import cz.muni.fi.storm.tools.readers.KafkaConsumer;
import cz.muni.fi.storm.tools.readers.Reader;

public class KafkaCounterSpout extends BaseRichSpout {
    
    private SpoutOutputCollector collector;
    private Reader kafkaConsumer;
    private ServiceCounter serviceCounter;
    private long totalCounter = 0;
    private int nullCount = 0;
    
    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();
        String broker = (String) stormConf.get("kafkaConsumer.broker");
        int port = new Integer(stormConf.get("kafkaConsumer.port").toString());
        String topic = (String) stormConf.get("kafkaConsumer.topic");
        
        this.kafkaConsumer = new KafkaConsumer(broker, port, topic, true, totalTasks, actualTask);
        this.serviceCounter = new ServiceCounter(collector, stormConf);
        this.collector = collector;
    }
    
    @Override
    public void nextTuple() {
        String flowJson = kafkaConsumer.next();
        if (flowJson != null) {
            serviceCounter.count();
            totalCounter++;
            
        } else {
            nullCount++;
            if (nullCount > 3) {
                collector.emit(new Values(totalCounter));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
        ServiceCounter.declareServiceStream(declarer);
    }
    
    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
