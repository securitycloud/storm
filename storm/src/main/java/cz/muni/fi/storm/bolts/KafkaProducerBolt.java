package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerBolt extends BaseBasicBolt {
    private static final Logger log = Logger.getLogger( KafkaProducerBolt.class.getName() );

    private Producer<String, String> producer;
    private String kafkaConsumerIp;
    
    public KafkaProducerBolt(String kafkaConsumerIp){
        log.fine("Initializing kafka Producer bolt with kafkaConsumer Ip from constructor");
        this.kafkaConsumerIp=kafkaConsumerIp;
    
    }
    public void prepare(Map stormConf, TopologyContext context) {
        log.fine("Prepare consumer KPB");
        Properties props = new Properties();
        props.put("zookeeper.connect","10.16.31.201:2181");
        props.put("metadata.broker.list", kafkaConsumerIp+":9092");
        props.put("broker.id","0");
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
        
        log.fine("Executing and sending data to new topic");
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
