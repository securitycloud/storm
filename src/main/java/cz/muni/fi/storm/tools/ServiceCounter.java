package cz.muni.fi.storm.tools;

import cz.muni.fi.storm.tools.writers.KafkaProducer;

public class ServiceCounter {

    private static final String topic = "storm-service";
    private KafkaProducer kafkaProducer;
    private int counter = 0;
    private long lastTime;
    
    public ServiceCounter(String broker, int port) {
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
    }
    
    public ServiceCounter(KafkaProducer otherKafkaProducer) {
        this.kafkaProducer = new KafkaProducer(otherKafkaProducer, topic);
    }
    
    public void count() {
        counter++;
        if (counter == 1000000) {
            counter = 0;
            long actualTime = System.currentTimeMillis();
            String interval = (actualTime - lastTime) + "";
            kafkaProducer.send(interval);
            lastTime = actualTime;
        }
    }
    
    public void close() {
        kafkaProducer.close();
    }
}
