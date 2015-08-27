package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.Map;

public class GlobalCounterBolt extends BaseRichBolt {
    
    private final int totalSenders;
    private int actualSenders = 0;
    private long count = 0;
    private KafkaProducer kafkaProducer;

    public GlobalCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
    }

    @Override
    public void execute(Tuple tuple) {
        long newCount = new Long(tuple.getValue(0).toString());
        count += newCount;
            
        actualSenders++;
        if (actualSenders == totalSenders) {
            kafkaProducer.send("Count is " + count);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    
    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
