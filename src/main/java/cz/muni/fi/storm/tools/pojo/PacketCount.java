package cz.muni.fi.storm.tools.pojo;

import scala.Serializable;

/**
 * POJO for mapping of parsed JSON packet counter.
 */
public class PacketCount implements Serializable {

    private String dst_ip_addr;
    private long packets;

    public String getDst_ip_addr() {
        return dst_ip_addr;
    }

    public void setDst_ip_addr(String dst_ip_addr) {
        this.dst_ip_addr = dst_ip_addr;
    }

    public long getPackets() {
        return packets;
    }

    public void setPackets(long packets) {
        this.packets = packets;
    }

    @Override
    public String toString() {
        return "PacketCount{dst_ip_addr=" + dst_ip_addr + ", packets=" + packets + "}";
    }
}
