package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.KafkaProducer;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;

public class KafkaProducerBolt extends BaseRichBolt {

    private String broker;
    private int port;
    private String topic;
    private KafkaProducer kafkaProducer;
    private ServiceCounter counter;

    public KafkaProducerBolt(String broker, int port, String topic) {
        this.broker = broker;
        this.port = port;
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        kafkaProducer = new KafkaProducer(broker, port, topic);
        counter = new ServiceCounter(kafkaProducer);
    }

    @Override
    public void execute(Tuple tuple) {
        kafkaProducer.send(tuple.getValue(0).toString());
        counter.count();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
