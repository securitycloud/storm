package cz.muni.fi.storm.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * These utilities help for build topology.
 */
public class TopologyUtil {
    
    private static final String stormPropertiesFile = "/storm.properties";
    
    /**
     * Loads properties from property file.
     * 
     * @return map of properties for topology configuration
     */
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
}
