package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON packet counter.
 */
public class PacketCount implements Serializable {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Integer rank;
    
    @JsonProperty("dest_ip_addr")
    private String destIpAddr;
    
    @JsonProperty("sum(packets)")
    private long packets;
    
    public Integer getRank() {
        return rank;
    }
    
    public void setRank(int rank) {
        this.rank = rank;
    }

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
