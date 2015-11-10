package cz.muni.fi.storm.tools.writers;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This writer able to write to kafka.
 */
public class KafkaProducer implements Writer {

    private final Producer<String, String> producer;
    private final String topic;

    /**
     * Open new kafka producer for this writer.
     * 
     * @param broker IP adress of kafka broker
     * @param port number of port of kafka broker
     * @param topic name of topic for kafka
     * @param isAsync true allow asynchronism write, otherwise is synchronism write
     */
    public KafkaProducer(String broker, int port, String topic, boolean isAsync) {
        Properties props = new Properties();
        props.put("metadata.broker.list", broker + ":" + port);
        props.put("broker.id", "0");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        if (isAsync) {
            props.put("producer.type", "async");
            props.put("batch.size", 5000);
        }
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
        this.topic = topic;
    }
    
    @Override
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
