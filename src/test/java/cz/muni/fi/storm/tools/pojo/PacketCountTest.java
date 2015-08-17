package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class PacketCountTest {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Test
    public void testSerialization() throws JsonProcessingException {
        String ip = "1.2.3.4";
        long packets = 42;
        int rank = 5;
        
        PacketCount packetCount = new PacketCount();
        packetCount.setDestIpAddr(ip);
        packetCount.setPackets(packets);
        String packetCountJson = mapper.writeValueAsString(packetCount);
        assertTrue(packetCountJson.contains("\"dest_ip_addr\":\"" + ip + "\""));
        assertTrue(packetCountJson.contains("\"sum(packets)\":" + packets));
        assertFalse(packetCountJson.contains("rank"));
        
        packetCount.setRank(rank);
        packetCountJson = mapper.writeValueAsString(packetCount);
        assertTrue(packetCountJson.contains("\"rank\":" + rank));
    }
    
    @Test
    public void testDeserialization() throws IOException {
        String ip = "2.3.4.5";
        long packets = 24;
        int rank = 3;
        String packetCountJson = "{\"rank\":" + rank + ","
                + "\"dest_ip_addr\":\"" + ip + "\","
                + "\"sum(packets)\":" + packets + "}";
        
        PacketCount packetCount = mapper.readValue(packetCountJson, PacketCount.class);
        assertTrue(ip.equals(packetCount.getDestIpAddr()));
        assertTrue(packets == packetCount.getPackets());
        assertTrue(rank == packetCount.getRank());
    }
}
