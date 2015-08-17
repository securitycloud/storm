package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON flow counter.
 */
public class FlowCount implements Serializable {
    
    @JsonProperty("src_ip_addr")
    private String srcIpAddr;
    
    @JsonProperty("sum(flows)")
    private long flows;
    
    @JsonProperty("src_ip_addr")
    public String getSrcIpAddr() {
        return srcIpAddr;
    }

    @JsonProperty("src_ip_addr")
    public void setSrcIpAddr(String srcIpAddr) {
        this.srcIpAddr = srcIpAddr;
    }

    @JsonProperty("count(flows)")
    public long getFlows() {
        return flows;
    }

    @JsonProperty("sum(flows)")
    public void setFlows(long flows) {
        this.flows = flows;
    }

    @Override
    public String toString() {
        return "PacketCount{"
                + "srcIpAddr=" + srcIpAddr + ", "
                + "flows=" + flows + "}";
    }
}
