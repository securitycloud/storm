package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
    private boolean isCountable;
    private int counter = 0;
    private long lastTime;
    private OutputCollector collector;

    public KafkaProducerBolt(String kafkaConsumerIp, String kafkaConsumerPort, String kafkaConsumerTopic, boolean isCountable) {
        this.kafkaConsumerIp = kafkaConsumerIp;
        this.kafkaConsumerPort = kafkaConsumerPort;
        this.kafkaConsumerTopic = kafkaConsumerTopic;
        this.isCountable = isCountable;
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
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafkaConsumerTopic, tuple.getValue(0).toString());
        producer.send(data);
        if (isCountable) {
            counter++;
            if (counter == 1000000) {
                counter = 0;
                long actualTime = System.currentTimeMillis();
                collector.emit("service", new Values((actualTime - lastTime) + ""));
                lastTime = actualTime;
            }
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (isCountable) {
            declarer.declareStream("service", new Fields("interval"));
        }   
    }

    @Override
    public void cleanup() {
        producer.close();
    }
}
