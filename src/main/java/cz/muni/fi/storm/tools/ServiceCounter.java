package cz.muni.fi.storm.tools;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ServiceCounter {

    private Producer<String, String> producer;
    private int counter = 0;
    private long lastTime;
    
    public ServiceCounter(String kafkaConsumerIp, String kafkaConsumerPort) {
        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaConsumerIp + ":2181");
        props.put("metadata.broker.list", kafkaConsumerIp + ":" + kafkaConsumerPort);
        props.put("broker.id", "0");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        props.put("producer.type", "async");
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
    }
    
    public ServiceCounter(Producer<String, String> producer) {
        this.producer = producer;
    }
    
    public void count() {
        counter++;
        if (counter == 1000000) {
            counter = 0;
            long actualTime = System.currentTimeMillis();
            String interval = (actualTime - lastTime) + "";
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("storm-service", interval);
            producer.send(data);
            lastTime = actualTime;
        }
    }
}
