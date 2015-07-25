package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.pojo.Flow;
import cz.muni.fi.storm.tools.pojo.PacketCount;
import java.io.IOException;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;
    private String onlyIp;
    
    public PacketCounterBolt() { }
    
    public PacketCounterBolt(String onlyIp) {
        this.onlyIp = onlyIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            PacketCount packetCount = new PacketCount();
            
            if (onlyIp != null ) {
                if (! onlyIp.equals(flow.getDst_ip_addr())) {
                    return;
                }
                packetCount.setDst_ip_addr(onlyIp);
            } else {
                packetCount.setDst_ip_addr(flow.getDst_ip_addr());
            }
            
            packetCount.setPackets(flow.getPackets());
            
            String packetCountJson = new ObjectMapper().writer().withDefaultPrettyPrinter()
                    .writeValueAsString(packetCount);
            collector.emit(new Values(packetCountJson));
        } catch (IOException e) {
            // nothing
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
