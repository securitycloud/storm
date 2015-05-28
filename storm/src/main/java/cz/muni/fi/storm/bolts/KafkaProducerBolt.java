package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerBolt extends BaseBasicBolt {

    private Producer<String, String> producer;
    private String kafkaConsumerIp;
    
    public KafkaProducerBolt(String kafkaConsumerIp){
        this.kafkaConsumerIp=kafkaConsumerIp;
    
    }
    public void prepare(Map stormConf, TopologyContext context) {
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaConsumerIp+":9092");
        props.put("broker.id","1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        //fileOut = new SimpleFileOutput((String)stormConf.get("outputFile"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute (Tuple tuple, BasicOutputCollector collector) {
        // This topic must be different kafka consumer topic.
        // Because run kafka consumer and producer on localhost, to be loop.
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("new-topic", tuple.toString());
        producer.send(data);
    }
    
    @Override
    public void cleanup(){
        producer.close();
    }
}
