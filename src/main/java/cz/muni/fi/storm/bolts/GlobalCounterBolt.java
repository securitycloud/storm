package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.Map;

/**
 * This global bolt counts final count from local bolts and sends result to kafka.
 * Output kafka topic is extracted from configuration of storm.
 */
public class GlobalCounterBolt extends BaseRichBolt {
    
    private final int totalSenders;
    private int actualSenders = 0;
    private long count = 0;
    private KafkaProducer kafkaProducer;

    /**
     * Constructor for new instance of global counter.
     * 
     * @param totalSenders number of local bolts which send count
     */
    public GlobalCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }
    
    /*
     * Requires parameters from storm configuration:
     * - kafkaProducer.broker ip address of output kafka broker
     * - kafkaProducer.port number of port of output kafka broker
     * - kafkaProducer.topic name of output kafka topic
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
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
