package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON packet counter.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IpCount implements Serializable {
    
    private Integer rank;
    
    @JsonProperty("src_ip_addr")
    private String srcIp;
    
    @JsonProperty("sum(packets)")
    private Integer packets;
    
    @JsonProperty("count(flows)")
    private Integer flows;
    
    public Integer getRank() {
        return rank;
    }
    public void setRank(Integer rank) {
        this.rank = rank;
    }

    @JsonProperty("src_ip_addr")
    public String getSrcIp() {
        return srcIp;
    }
    @JsonProperty("src_ip_addr")
    public void setSrcIpAddr(String srcIp) {
        this.srcIp = srcIp;
    }

    @JsonProperty("sum(packets)")
    public Integer getPackets() {
        return packets;
    }
    @JsonProperty("sum(packets)")
    public void setPackets(Integer packets) {
        this.packets = packets;
    }
    
    @JsonProperty("count(flows)")
    public Integer getFlows() {
        return flows;
    }
    @JsonProperty("count(flows)")
    public void setFlows(Integer flows) {
        this.flows = flows;
    }
}
