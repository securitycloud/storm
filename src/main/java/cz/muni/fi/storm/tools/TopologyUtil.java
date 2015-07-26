package cz.muni.fi.storm.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class TopologyUtil {
    
    private static final String stormPropertiesFile = "/storm.properties";
    
    public Map<String, Object> loadProperties() {
        try {
            InputStream inputStream = getClass().getResourceAsStream(stormPropertiesFile);
            Properties properties = new Properties();
            properties.load(inputStream);
            for (String property : properties.stringPropertyNames()) {
                String value = properties.getProperty(property);
                try {
                    properties.put(property, Integer.valueOf(value));
                } catch (NumberFormatException e) {
                    // nothing
                }
            }
            return (Map) properties;
            
        } catch (IOException e) {
            throw new RuntimeException("Properties file is corrupted", e);
        }
    }
}
