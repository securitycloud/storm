package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON counter for ips.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IpCount implements Serializable {
    
    /**
     * Rank of sorting ips by count.
     */
    private Integer rank;
    
    @JsonProperty("src_ip_addr")
    private String srcIp;
    
    private Integer count;
    
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

    public Integer getCount() {
        return count;
    }
    public void setCount(Integer count) {
        this.count = count;
    }
}
