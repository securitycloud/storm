package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;
import cz.muni.fi.storm.tools.readers.KafkaConsumer;
import cz.muni.fi.storm.tools.readers.Reader;

public class KafkaCounterSpout extends BaseRichSpout {
    
    private Reader kafkaConsumer;
    private ServiceCounter counter;
    
    @Override
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();
        String broker = (String) stormConf.get("kafkaConsumer.broker");
        int port = new Integer(stormConf.get("kafkaConsumer.port").toString());
        String topic = (String) stormConf.get("kafkaConsumer.topic");
        
        this.kafkaConsumer = new KafkaConsumer(broker, port, topic, true, totalTasks, actualTask);
        this.counter = new ServiceCounter(collector, stormConf);
    }
    
    @Override
    public void nextTuple() {
        String flowJson = kafkaConsumer.next();
        if (flowJson != null) {
            counter.count();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ServiceCounter.declareServiceStream(declarer);
    }
    
    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
