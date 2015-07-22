package cz.muni.fi.storm.tools;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer implements AutoCloseable {

    private Producer<String, String> producer;
    private String topic;

    public KafkaProducer(String broker, int port, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        //props.put("zookeeper.connect", kafkaConsumerIp + ":2181");
        props.put("metadata.broker.list", broker + ":" + port);
        props.put("broker.id", "0");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        props.put("producer.type", "async");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }
    
    public KafkaProducer(KafkaProducer otherFakfaProducer, String topic) {
        this.producer = otherFakfaProducer.getProducer();
        this.topic = topic;
    }
    
    protected Producer<String, String> getProducer() {
        return producer;
    }
    
    public void send(String message) {
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, message);
        producer.send(keyedMessage);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
