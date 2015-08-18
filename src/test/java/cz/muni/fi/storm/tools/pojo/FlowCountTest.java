package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class FlowCountTest {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        String ip = "1.2.3.4";
        long flows = 42;
        
        FlowCount flowCount = new FlowCount();
        flowCount.setSrcIpAddr(ip);
        flowCount.setFlows(flows);
        String flowCountJson = mapper.writeValueAsString(flowCount);
        assertTrue(flowCountJson.contains("\"src_ip_addr\":\"" + ip + "\""));
        assertTrue(flowCountJson.contains("\"count(flows)\":" + flows));
    }
    
    @Test
    public void testDeserialization() throws IOException {
        String ip = "2.3.4.5";
        long flows = 24;
        String flowCountJson = "{\"src_ip_addr\":\"" + ip + "\","
                                + "\"count(flows)\":" + flows + "}";
        
        FlowCount flowCount = mapper.readValue(flowCountJson, FlowCount.class);
        assertTrue(ip.equals(flowCount.getSrcIpAddr()));
        assertTrue(flows == flowCount.getFlows());
    }
}
