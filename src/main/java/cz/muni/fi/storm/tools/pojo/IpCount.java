package cz.muni.fi.storm.tools.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import scala.Serializable;

/**
 * POJO for mapping of parsed JSON counter for IPs.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IpCount implements Serializable {
    
    /**
     * Rank of sorting IPs by count.
     */
    private Integer rank;
    
    @JsonProperty("src_ip_addr")
    private String srcIp;
    
    @JsonProperty("dst_ip_addr")
    private String dstIp;
    
    private Integer count;
    
    private Integer kilobytes;
    
    private List<IpCount> destinations;
    
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
    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }
    
    @JsonProperty("dst_ip_addr")
    public String getDstIp() {
        return dstIp;
    }
    @JsonProperty("dst_ip_addr")
    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public Integer getCount() {
        return count;
    }
    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getKilobytes() {
        return kilobytes;
    }
    public void setKilobytes(Integer kilobytes) {
        this.kilobytes = kilobytes;
    }
    
    public List<IpCount> getDestinations() {
        return destinations;
    }
    public void setDestinations(List destinations) {
        this.destinations = destinations;
    }
}
