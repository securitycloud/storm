package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.Map;

/**
 * This bolt reads every flow and counts number of packets for chosen source ip.
 * Final number of packets is emitted next to a global bolt.
 */
public class FilterPacketCounterBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private long packetCounter;
    private ServiceCounter serviceCounter;
    private String srcIp;

    /*
     * Requires parameters from storm configuration:
     * - filter.srcIp ip address which packets are counted
     * - serviceCounter.messagesPerTopic requires service counter
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.serviceCounter = new ServiceCounter(stormConf, context);
        this.srcIp = (String) stormConf.get("filter.srcIp");
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();

        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            String ip = flow.getSrc_ip_addr();
            if (srcIp.equals(ip)) {
                packetCounter += flow.getPackets();
            }
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow <" + flowJson + ">", e);
        }
        
        if (serviceCounter.isEnd()) {
            collector.emit(new Values(packetCounter));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
