package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON packet counter.
 */
public class PacketCount implements Serializable {

    @JsonProperty("dest_ip_addr")
    private String destIpAddr;
    
    @JsonProperty("sum(packets)")
    private long packets;

    @JsonProperty("dest_ip_addr")
    public String getDestIpAddr() {
        return destIpAddr;
    }

    @JsonProperty("dest_ip_addr")
    public void setDestIpAddr(String destIpAddr) {
        this.destIpAddr = destIpAddr;
    }

    @JsonProperty("sum(packets)")
    public long getPackets() {
        return packets;
    }

    @JsonProperty("sum(packets)")
    public void setPackets(long packets) {
        this.packets = packets;
    }

    @Override
    public String toString() {
        return "PacketCount{destIpAddr=" + destIpAddr + ", packets=" + packets + "}";
    }
}
