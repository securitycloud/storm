package cz.muni.fi.storm.tools;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TopologyUtil {
    
    private static final String stormPropertiesFile = "/storm.properties";
    
    public Map<String, Object> loadProperties() {
        try {
            InputStream inputStream = getClass().getResourceAsStream(stormPropertiesFile);
            Properties properties = new Properties();
            properties.load(inputStream);
            
            if (properties.getProperty("kafkaProducer.topic")
                        .equals(properties.getProperty("kafkaConsumer.topic"))
                    && properties.getProperty("kafkaProducer.broker")
                        .equals(properties.getProperty("kafkaConsumer.broker"))) {
                throw new IllegalArgumentException(
                        "It creates loop! Please differnet kafkas brokers or topics");
            }
            
            for (String property : properties.stringPropertyNames()) {
                String value = properties.getProperty(property);
                try {
                    properties.put(property, Integer.valueOf(value));
                } catch (NumberFormatException e) {
                    if (value.equalsIgnoreCase("true")) {
                        properties.put(property, true);
                    }
                    if (value.equalsIgnoreCase("false")) {
                        properties.put(property, false);
                    }
                }
            }
            return (Map) properties;
            
        } catch (IOException e) {
            throw new RuntimeException("Properties file is corrupted", e);
        }
    }
    
    public static SpoutConfig getKafkaSpoutConfig(Config config) {
        String zookeeper = (String) config.get("kafkaConsumer.zookeeper");
        String broker = (String) config.get("kafkaConsumer.broker");
        ZkHosts zkHosts = new ZkHosts(broker);
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, zookeeper, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme() {
            @Override
            public Fields getOutputFields() {
                return new Fields("flow");
            }
        });
        kafkaConfig.forceFromStart = true;
        return kafkaConfig;
    }
}
