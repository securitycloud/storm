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

    private static final Logger log = Logger.getLogger(KafkaProducerBolt.class.getName());
    private Producer<String, String> producer;
    private String kafkaConsumerIp;
    private String kafkaConsumerPort;
    private String kafkaConsumerTopic;

    public KafkaProducerBolt(String kafkaConsumerIp, String kafkaConsumerPort, String kafkaConsumerTopic) {
        log.fine("Initializing kafka Producer bolt with kafkaConsumer ip, port and topic from constructor");
        this.kafkaConsumerIp = kafkaConsumerIp;
        this.kafkaConsumerPort = kafkaConsumerPort;
        this.kafkaConsumerTopic = kafkaConsumerTopic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.fine("Prepare consumer kafka producer bolt");
        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaConsumerIp + ":2181");
        props.put("metadata.broker.list", kafkaConsumerIp + ":" + kafkaConsumerPort);
        props.put("broker.id", "0");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        log.fine("Executing and sending data to topic");
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafkaConsumerTopic, tuple.toString());
        producer.send(data);
    }

    @Override
    public void cleanup() {
        producer.close();
    }
}
