package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerBolt extends BaseRichBolt {

    private Producer<String, String> producer;
    private String kafkaConsumerIp;
    private String kafkaConsumerPort;
    private String kafkaConsumerTopic;
    private ServiceCounter counter;

    public KafkaProducerBolt(String kafkaConsumerIp, String kafkaConsumerPort, String kafkaConsumerTopic) {
        this.kafkaConsumerIp = kafkaConsumerIp;
        this.kafkaConsumerPort = kafkaConsumerPort;
        this.kafkaConsumerTopic = kafkaConsumerTopic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaConsumerIp + ":2181");
        props.put("metadata.broker.list", kafkaConsumerIp + ":" + kafkaConsumerPort);
        props.put("broker.id", "0");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        props.put("producer.type", "async");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        counter = new ServiceCounter(producer);
    }

    @Override
    public void execute(Tuple tuple) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafkaConsumerTopic, tuple.getValue(0).toString());
        producer.send(data);
        counter.count();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

    @Override
    public void cleanup() {
        producer.close();
    }
}
